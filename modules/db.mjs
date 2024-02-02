import {
    of,
    switchMap,
    combineLatest,
    map,
    shareReplay,
    distinct,
    mergeMap,
    filter,
    from,
    scan,
    delay,
    reduce,
    withLatestFrom,
    tap,
    merge,
    ignoreElements,
} from "rxjs";

import { Graph, alg } from "@dagrejs/graphlib";

import { createRxDatabase, addRxPlugin } from "rxdb";
import { getRxStorageDexie } from "rxdb/plugins/storage-dexie";
import { getRxStorageMemory } from "rxdb/plugins/storage-memory";
import { RxDBLocalDocumentsPlugin } from "rxdb/plugins/local-documents";
import { RxDBDevModePlugin } from "rxdb/plugins/dev-mode";
import { v4 as uuidv4 } from "uuid";
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
    statics: {},
};

const triggersSchema = {
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
            ref: "nodes",
            maxLength: 255,
        },
        node: {
            type: "string",
            ref: "nodes",
            maxLength: 255,
        },
        session: {
            type: "string",
        },
        root: {
            type: "string",
        },
        parents: {
            type: "array",
            ref: "evaluations",
            items: {
                type: "string",
                maxLength: 255,
            },
        },
    },
    required: ["id", "flow", "node"],
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
            schema: nodesSchema,
            methods: {
                triggers$: function (session) {
                    // console.log(
                    //     "CALL TRIGGER",
                    //     this.flow,
                    //     session,
                    //     this.parents
                    // );
                    const evaluations = this.collection.database.evaluations;
                    const parents$ = this.get$("parents").pipe(
                        filter(Boolean),
                        shareReplay(1)
                    );
                    const trees$ = evaluations
                        .find({
                            selector: {
                                session,
                                flow: this.flow,
                                parents: {
                                    $exists: false,
                                },
                            },
                        })
                        .$.pipe(
                            switchMap((roots) =>
                                from(roots).pipe(
                                    // tap((root) =>
                                    //     console.log("got root", root.root)
                                    // ),
                                    mergeMap(
                                        (root) =>
                                            evaluations.find({
                                                selector: {
                                                    root: root.root,
                                                    complete: true,
                                                },
                                            }).$
                                    ),
                                    filter((r) => r.length),
                                    mergeMap((sameRoot) =>
                                        from(sameRoot).pipe(
                                            reduce((graph, evaluation) => {
                                                graph.setNode(
                                                    evaluation.id,
                                                    evaluation
                                                );
                                                const parents =
                                                    evaluation.parents || [];
                                                for (const parent of parents) {
                                                    graph.setEdge(
                                                        evaluation.id,
                                                        parent
                                                    );
                                                }
                                                return graph;
                                            }, new Graph())
                                        )
                                    ),
                                    scan((all, graph) => {
                                        const node = graph.node(
                                            graph.sinks()[0]
                                        );
                                        // console.log(
                                        //     "add tree",
                                        //     graph.nodes(),
                                        //     graph.edges(),
                                        //     node.root
                                        // );
                                        all.set(node.root, graph);
                                        return all;
                                    }, new Map())
                                )
                            ),
                            shareReplay(1)
                        );

                    // return of(null);

                    const triggerInsert$ = parents$.pipe(
                        // tap((parents) =>
                        //     console.log(this.id, "parents", parents.length)
                        // ),
                        switchMap(
                            (ids) =>
                                evaluations.find({
                                    selector: {
                                        session,
                                        node: {
                                            $in: ids,
                                        },
                                        complete: true,
                                    },
                                }).$
                        ),
                        mergeMap((newEvaluations) => from(newEvaluations)),
                        // tap((nEval) =>
                        //     console.log("NEW EVAL, nEval.id", nEval.id)
                        // ),
                        delay(100),
                        withLatestFrom(trees$, parents$),
                        // tap(
                        //     console.log.bind(
                        //         console,
                        //         "got new, trees, and parents$"
                        //     )
                        // ),
                        filter(([evaluation, trees]) => {
                            // console.log(
                            //     "check trees for root",
                            //     evaluation.root,
                            //     trees
                            // );
                            const tree = trees.get(evaluation.root);
                            return tree.node(evaluation.id);
                        }),
                        map(([evaluation, trees, parents]) => {
                            const tree = trees.get(evaluation.root);
                            const mst = alg.prim(tree, () => 1);
                            // console.log(tree);
                            const djk = alg.dijkstra(
                                tree,
                                evaluation.id,
                                () => 1
                            );
                            const djkArray = Object.keys(djk)
                                .map((k) => ({
                                    to: k,
                                    ...djk[k],
                                }))
                                .sort((a, b) => a.distance - b.distance);

                            // console.log(
                            //     "djk array",
                            //     evaluation.id,
                            //     djkArray,
                            //     alg.topsort(tree),
                            //     alg.topsort(mst)
                            // );
                            const triggers = new Map([
                                [evaluation.node, evaluation],
                            ]);
                            for (const { to: evalId } of djkArray) {
                                const evalNode = tree.node(evalId);
                                if (
                                    parents.includes(evalNode.node) &&
                                    !triggers.has(evalNode.node)
                                ) {
                                    triggers.set(evalNode.node, evalNode);
                                    if (
                                        parents.every((parent) =>
                                            triggers.has(parent)
                                        )
                                    ) {
                                        break;
                                    }
                                }
                            }
                            // console.log(
                            //     "triggers",
                            //     Array.from(
                            //         triggers.values().map(({ id }) => id)
                            //     )
                            // );
                            return [evaluation, triggers, parents];
                        }),
                        // tap(() => console.log("ABOUT TO FILTER TRIGGERS")),
                        filter(([_, triggers, parents]) =>
                            parents.every((p) => triggers.has(p))
                        ),
                        // tap(() => console.log("FINISHED FILTER TRIGGERS")),
                        distinct(([_, triggers]) =>
                            JSON.stringify(
                                Array.from(
                                    triggers.values().map(({ id }) => id)
                                ).sort()
                            )
                        ),
                        // tap(() => console.log("PASSED DISTINCT")),

                        tap(async ([evaluation, triggers]) => {
                            const trigger =
                                await this.collection.database.triggers.upsert({
                                    id: uuidv4(),
                                    flow: this.flow,
                                    node: this.id,
                                    root: evaluation.root,
                                    session,
                                    parents: Array.from(
                                        triggers.values().map(({ id }) => id)
                                    ),
                                });

                            // console.log("MADE TRIGGER");
                            return trigger;
                        }),
                        // tap(() => console.log("WROTE NEW TRIGGER")),
                        ignoreElements()
                    );

                    const triggers$ = this.collection.database.triggers
                        .find({
                            selector: {
                                node: this.id,
                                session,
                                flow: this.flow,
                            },
                        })
                        .$.pipe(
                            filter(Boolean),
                            mergeMap((res) => from(res)),
                            distinct(({ id }) => id)
                            // tap((t) => console.log("GOT TRIGGER", t.id))
                        );

                    return merge(triggerInsert$, triggers$);
                },
            },
        },
        evaluations: {
            schema: evaluationsSchema,
            methods: {
                getContext: async function () {
                    return await partial(this);
                },
            },
        },
        triggers: {
            schema: triggersSchema,
            methods: {
                createEvals: function (qty = 1) {
                    return from(new Array(qty)).pipe(
                        mergeMap(() => this.createEval())
                    );
                },
                createEval: function () {
                    // console.log("CREATE EVAL", this.root);
                    return from(
                        this.collection.database.evaluations.upsert({
                            id: uuidv4(),
                            flow: this.flow,
                            node: this.node,
                            root: this.root || this.id,
                            session: this.session,
                            parents: this.parents,
                        })
                    );
                },
            },
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

    return (
        await Promise.all(
            alg.topsort(graph).map((id) => graph.node(id).getLatest())
        )
    ).map((n) => n.toJSON());
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

const flowid = "FLOW";
const session = new Date().toDateString();

// await db.evaluations.bulkUpsert([
//     {
//         id: "A",
//         node: "node_A",
//         root: "A",
//         complete: true,
//         flow: flowid,
//         session: session,
//     },
//     {
//         id: "B",
//         node: "node_B",
//         root: "A",
//         parents: ["A"],
//         complete: true,
//         flow: flowid,
//         session: session,
//     },
//     {
//         id: "C",
//         node: "node_C",
//         root: "A",
//         parents: ["A"],
//         complete: true,
//         flow: flowid,
//         session: session,
//     },
//     {
//         id: "D",
//         node: "node_D",
//         root: "A",
//         parents: ["B", "C"],
//         complete: true,
//         flow: flowid,
//         session: session,
//     },
//     {
//         id: "E",
//         node: "node_E",
//         root: "A",
//         parents: ["D"],
//         complete: true,
//         flow: flowid,
//         session: session,
//     },
// ]);

// const node = await db.nodes.upsert({
//     id: "node_E",
//     parents: ["node_D", "node_C", "node_B"],
//     flow: flowid,
// });

// node.triggers$(session).subscribe(async (newEval) => {
//     console.log("newTrigger", newEval);
//     console.log(
//         "newTrigger",
//         await (await firstValueFrom(newEval.createEval())).getContext()
//     ); //, newEval.toJSON());
// });

// node.collection.database.triggers.upsert({
//     id: uuidv4(),
//     node: node.id,
//     flow: flowid,
//     session,
// });

// await db.evaluations.bulkUpsert([
//     {
//         id: "D2",
//         node: "node_D",
//         root: "A",
//         parents: ["B", "C"],
//         complete: true,
//         flow: flowid,
//         session: session,
//     },
//     {
//         id: "C2",
//         node: "node_C",
//         root: "A",
//         parents: ["A"],
//         complete: true,
//         flow: flowid,
//         session: session,
//     },
//     {
//         id: "B2",
//         node: "node_B",
//         root: "A",
//         parents: ["A"],
//         complete: true,
//         flow: flowid,
//         session: session,
//     },
// ]);

// // const root = await db.evaluation.findOne({ selector: { id: "A" } }).exec();

// // fullTree(root).subscribe((tree) =>
// //     console.log("tree", JSON.stringify(tree, null, 2))
// // );

// // const G = await db.evaluation.findOne({ selector: { id: "D" } }).exec();
// // const isa = await partial(G);
// // console.log("G isa", isa);
