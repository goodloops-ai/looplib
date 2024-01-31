import { of, switchMap, combineLatest, map } from "rxjs";

import { Graph, alg } from "@dagrejs/graphlib";

import { createRxDatabase, addRxPlugin } from "rxdb";
import { getRxStorageDexie } from "rxdb/plugins/storage-dexie";
import { getRxStorageMemory } from "rxdb/plugins/storage-memory";
import { RxDBLocalDocumentsPlugin } from "rxdb/plugins/local-documents";
import { RxDBDevModePlugin } from "rxdb/plugins/dev-mode";
addRxPlugin(RxDBDevModePlugin);
addRxPlugin(RxDBLocalDocumentsPlugin);

const evaluationsSchema = {
    version: 0,
    type: "object",
    primaryKey: "id",
    properties: {
        id: {
            type: "string",
            maxLength: 255,
        },
        session: {
            type: "string",
        },
        node: {
            type: "string",
            ref: "nodes",
            maxLength: 255,
        },
        flow: {
            type: "string",
            maxLength: 255,
        },
        parents: {
            type: "array",
            ref: "evaluations",
            items: {
                type: "string",
            },
        },
        state: {
            type: "object",
            additionalProperties: true,
        },
        complete: {
            type: "boolean",
            default: false,
        },
        previous: {
            type: "string",
            ref: "evaluations",
        },
        root: {
            type: "string",
            ref: "evaluations",
        },
        packets: {
            type: "array",
            items: {
                type: "object",
                properties: {
                    type: {
                        type: "string",
                    },
                    data: {
                        type: "object",
                        additionalProperties: true,
                    },
                },
            },
        },
    },
    required: ["id", "flow", "node"],
    indexes: ["flow", "node"],
};

const nodesSchema = {
    version: 0,
    type: "object",
    primaryKey: "id",
    properties: {
        id: {
            type: "string",
            maxLength: 255,
        },
        flow: {
            type: "string",
        },
        parents: {
            type: "array",
            ref: "nodes",
            items: {
                type: "string",
            },
        },
        operator: {
            type: "string",
        },
        config: {
            type: "object",
            additionalProperties: true,
        },
        // Add other properties as needed
    },
    required: ["id", "flow"],
};

const connectionSchema = {
    version: 0,
    type: "object",
    primaryKey: "id",
    properties: {
        id: {
            type: "string",
            maxLength: 255,
        },
        flow: {
            type: "string",
            maxLength: 255,
        },
        source: {
            type: "string",
            maxLength: 255,
        },
        target: {
            type: "string",
            maxLength: 255,
        },
        connect: {
            type: "string",
        },
        config: {
            type: "object",
            additionalProperties: true,
        },
        // Add other properties as needed
    },
    required: ["id", "flow", "source", "target"],
    indexes: ["source", "target", "flow"],
};

const config = {
    dbName: "flowdb",
    collections: {
        nodes: {
            schema: nodeSchema,
        },
        evaluations: {
            schema: evaluationSchema,
        },
    },
};

export const db = await initializeDatabase(config).catch(console.error);

async function initializeDatabase(config) {
    let db;
    if (typeof indexedDB !== "undefined") {
        db = await createRxDatabase({
            name: config.dbName,
            storage: getRxStorageDexie(),
            localDocuments: true,
        });
    } else {
        db = await createRxDatabase({
            name: config.dbName,
            storage: getRxStorageMemory(),
            localDocuments: true,
        });
    }

    await db.addCollections(config.collections);

    return db;
}

export function queryThread(doc) {
    return doc.collection
        .find({
            selector: {
                parent: doc.id,
                packets: {
                    $exists: true,
                },
            },
        })
        .$.pipe(
            switchMap((children) => {
                if (!children?.length) {
                    // console.log('no children');
                    return of([]);
                } else if (children.length === 1) {
                    // console.log('1 child');
                    return queryThread(children[0]);
                } else {
                    // console.log('2 children');
                    return combineLatest(children.map(queryThread)).pipe(
                        map((gc) => [gc])
                    );
                }
            })
        );
}
export const fullTree = (doc, seen = new Set()) => {
    return doc.collection
        .find({
            selector: {
                root: doc.id,
                complete: true,
            },
        })
        .$.pipe(
            map((nodes) => {
                const graph = new Graph();
                for (const node of nodes) {
                    graph.setNode(node.id, node);
                }

                for (const node of nodes) {
                    const parents = node.parents || [];
                    for (const parent of parents) {
                        graph.setEdge(parent, node.id);
                    }
                }

                return alg.topsort(graph).map((id) => graph.node(id).toJSON());
            })
        );
};

const partial = async (output, graph = new Graph()) => {
    graph.setNode(output.id, output);
    const parents = (await output.parents_) || [];

    const proms = [];
    for (const parent of parents) {
        graph.setEdge(parent.id, output.id);
        proms.push(partial(parent, graph));
    }

    await Promise.all(proms);

    return Promise.all(
        alg.topsort(graph).map((id) => graph.node(id).getLatest())
    );
};

const shouldInclude = async (doc, ancestorId, depth = 0) => {
    let parents = await doc.parents_;
    if (!depth) {
        parents = parents.filter(({ id }) => id !== ancestorId);
    }

    if (!parents?.length) {
        return false;
    }

    if (depth && parents.some((doc) => doc.id === ancestorId)) {
        return true;
    }

    return (
        await Promise.all(
            parents.map((parent) => shouldInclude(parent, ancestorId, ++depth))
        )
    ).reduce((found, path) => found || path, false);
};

await db.evaluation.bulkUpsert([
    {
        id: "A",
        root: "A",
        complete: true,
    },
    {
        id: "B",
        root: "A",
        parents: ["A"],
        complete: true,
    },
    {
        id: "C",
        root: "A",
        parents: ["A"],
        complete: true,
    },
    {
        id: "D",
        root: "A",
        parents: ["B", "C"],
        complete: true,
    },
    {
        id: "E",
        root: "A",
        parents: ["D"],
        complete: true,
    },
]);
const root = await db.evaluation.findOne({ selector: { id: "A" } }).exec();

fullTree(root).subscribe((tree) =>
    console.log("tree", JSON.stringify(tree, null, 2))
);

const G = await db.evaluation.findOne({ selector: { id: "D" } }).exec();
const isa = await partial(G);
console.log("G isa", isa);
