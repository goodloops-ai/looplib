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
        trigger: {
            type: "string",
            ref: "triggers",
        },
        terminal: {
            type: "boolean",
            default: false,
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
        join: {
            type: "string",
            ref: "nodes",
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
                isAncestorOf: async function (node, seen = new Set()) {
                    // console.log("isAncestorOf", this.id, node.id);
                    const parents = ((await node.parents_) || []).filter(
                        (p) => !seen.has(p.id)
                    );
                    parents.forEach(({ id }) => seen.add(id));

                    if (!parents?.length) {
                        return false;
                    }
                    if (parents.some(({ id }) => id === this.id)) {
                        return true;
                    }

                    return (
                        await Promise.all(
                            parents.map((parent) =>
                                this.isAncestorOf(parent, seen)
                            )
                        )
                    ).reduce((acc, item) => acc || item);
                },
                isOutput: async function () {
                    const children = await this.collection
                        .find({
                            selector: {
                                parents: {
                                    $in: [this.id],
                                },
                            },
                        })
                        .exec();

                    // console.log("IS OUTPUT?", this.id, !children?.length);

                    return !children?.length;
                },
                triggers$: function (session) {
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
                                        all.set(node.root, graph);
                                        return all;
                                    }, new Map())
                                )
                            ),
                            shareReplay(1)
                        );

                    const triggerInsert$ = parents$.pipe(
                        switchMap(
                            (ids) =>
                                evaluations.find({
                                    selector: {
                                        session,
                                        node: {
                                            $in: ids,
                                        },
                                        complete: true,
                                        terminal: false,
                                    },
                                }).$
                        ),
                        mergeMap((newEvaluations) => from(newEvaluations)),
                        delay(100),
                        withLatestFrom(trees$, parents$),
                        filter(([evaluation, trees]) => {
                            // console.log(
                            //     "check trees for root",
                            //     evaluation.root,
                            //     trees
                            // );
                            const tree = trees.get(evaluation.root);
                            return tree.node(evaluation.id);
                        }),
                        mergeMap(async ([evaluation, trees, parents]) => {
                            const tree = trees.get(evaluation.root);
                            const necessaryParents = [];
                            for (const parentId of parents) {
                                const parent =
                                    await evaluation.collection.database.nodes
                                        .findOne({
                                            selector: {
                                                id: parentId,
                                            },
                                        })
                                        .exec();
                                if (!(await this.isAncestorOf(parent))) {
                                    // console.log(
                                    //     parent.id,
                                    //     "is necessary parent of",
                                    //     this.id
                                    // );
                                    necessaryParents.push(parent.id);
                                }
                            }
                            // const mst = alg.prim(tree, () => 1);
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
                            //     alg.topsort(tree)
                            // );
                            const triggers = new Map([
                                [evaluation.node, [evaluation]],
                            ]);
                            let allAccountedFor = false;
                            const run = Math.random();
                            for (const { to: evalId } of djkArray) {
                                const evalNode = tree.node(evalId);
                                if (evalNode.terminal) {
                                    continue;
                                }
                                if (parents.includes(evalNode.node)) {
                                    const evals =
                                        triggers.get(evalNode.node) || [];

                                    if (this.join || !evals.length) {
                                        evals.push(evalNode);
                                    }
                                    triggers.set(evalNode.node, evals);
                                    if (
                                        necessaryParents.every((parent) =>
                                            triggers.has(parent)
                                        )
                                    ) {
                                        if (!this.join) {
                                            break;
                                        } else {
                                            allAccountedFor = true;
                                            // console.log(
                                            //     "CHECK ALL ACCOUNTED FOR",
                                            //     run
                                            // );
                                            for (const [
                                                parent,
                                                set,
                                            ] of triggers.entries()) {
                                                const joinTrigger =
                                                    await set[0].getTrigger(
                                                        this.join
                                                    );
                                                if (!joinTrigger) {
                                                    console.log(
                                                        "DID NOT FIND JOIN TRIGGER"
                                                    );
                                                    continue;
                                                }

                                                const joinAncestors =
                                                    await evaluation.collection
                                                        .find({
                                                            selector: {
                                                                trigger:
                                                                    joinTrigger,
                                                            },
                                                        })
                                                        .exec();

                                                console.log(
                                                    "JOIN ANCESTORS",
                                                    run,
                                                    joinAncestors.map(
                                                        ({ id }) => id
                                                    ),
                                                    set
                                                );
                                                for (const ancestor of joinAncestors) {
                                                    const accountedFor =
                                                        await ancestor.isAccountedFor(
                                                            set
                                                        );
                                                    // console.log(
                                                    //     "ACCOUNTED FOR",
                                                    //     run,
                                                    //     evaluation.id,
                                                    //     ancestor.id,
                                                    //     accountedFor,
                                                    //     set.map(({ id }) => id)
                                                    // );
                                                    allAccountedFor =
                                                        allAccountedFor &&
                                                        accountedFor;
                                                }

                                                // find which sets have joinTrigger in their ancestry
                                                // accumulate node path for that join history
                                                // find all direct descendants of joinTrigger
                                                // follow node path back for each joinTrigger descendent. either to a termination or to a member of the current trigger set.
                                                //
                                            }

                                            if (allAccountedFor) {
                                                console.log(
                                                    "ALL ACCOUNTED?",
                                                    run,
                                                    allAccountedFor
                                                );
                                                break;
                                            }
                                        }
                                    }
                                }
                            }
                            // console.log(
                            //     "triggers",
                            //     Array.from(
                            //         triggers.values().map(({ id }) => id)
                            //     )
                            // );
                            return [
                                evaluation,
                                triggers,
                                necessaryParents,
                                allAccountedFor,
                            ];
                        }),
                        // tap(() => console.log("ABOUT TO FILTER TRIGGERS")),
                        filter(([_, triggers, parents, aaf]) => {
                            if (!this.join) {
                                return parents.every((p) => triggers.has(p));
                            } else {
                                return (
                                    aaf && parents.every((p) => triggers.has(p))
                                );
                            }
                        }),
                        // tap(() => console.log("FINISHED FILTER TRIGGERS")),
                        map(([evaluation, triggers]) => [
                            evaluation,
                            Array.from(
                                new Set(
                                    Array.from(triggers.values())
                                        .flat()
                                        .map(({ id }) => id)
                                        .sort()
                                )
                            ),
                        ]),
                        distinct(([_, triggers]) => JSON.stringify(triggers)),
                        // tap(() => console.log("PASSED DISTINCT")),

                        tap(async ([evaluation, parents]) => {
                            console.log;
                            const trigger =
                                await this.collection.database.triggers.upsert({
                                    id: uuidv4(),
                                    flow: this.flow,
                                    node: this.id,
                                    root: evaluation.root,
                                    session,
                                    parents,
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
                getTrigger: async function (_node) {
                    const context = await this.getContext();
                    const match = context
                        .reverse()
                        .find(({ node }) => node === _node);

                    return match?.trigger;
                },
                isAccountedFor: async function (potentialDescendants) {
                    if (this.terminal) {
                        console.log("FOUND TERMINAL");
                        return true;
                    }
                    const node = await this.node_;

                    if (await node.isOutput()) {
                        console.log("FOUND OUTPUT");
                        return true;
                    }

                    const search = await this.collection
                        .find({
                            selector: {
                                parents: {
                                    $in: [this.id],
                                },
                                complete: true,
                            },
                        })
                        .exec();

                    if (!search.length) {
                        console.log("WAITING FOR CHILD");
                        return false;
                    }

                    if (
                        search.some(({ id }) =>
                            potentialDescendants.some((desc) => desc.id === id)
                        )
                    ) {
                        console.log("FOUND IN SET");
                        return true;
                    }

                    const next = await Promise.all(
                        search.map((_eval) =>
                            _eval.isAccountedFor(potentialDescendants)
                        )
                    );

                    if (
                        next.length &&
                        next.reduce((acc, accounted) => acc && accounted, true)
                    ) {
                        return true;
                    }

                    return false;
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
                            trigger: this.id,
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

const partial = async (output, graph = new Graph()) => {
    graph.setNode(output.id, output);
    const parents = (await output.parents_) || [];

    const proms = [];
    for (const parent of parents) {
        graph.setEdge(parent.id, output.id);
        proms.push(partial(parent, graph));
    }

    if (!parents.length) {
        const trigger = await output.trigger_;
        graph.setNode(trigger.id, trigger);
        graph.setEdge(trigger.id, output.id);
    }

    await Promise.all(proms);

    return (
        await Promise.all(
            alg.topsort(graph).map((id) => graph.node(id).getLatest())
        )
    ).map((n) => n.toJSON());
};
