import {
    pipe,
    Subject,
    merge,
    combineLatest,
    scan,
    from,
    zip,
    withLatestFrom,
    tap,
    filter,
    map,
    switchMap,
    shareReplay,
    ReplaySubject,
    distinctUntilChanged,
    concatMap,
    takeUntil,
    catchError,
    startWith,
    ignoreElements,
    EMPTY,
    of,
    mergeMap,
    skip,
    mergeMap,
} from "rxjs";
import { deepEqual } from "fast-equals";

import { z } from "zod";
import { v4 as uuidv4 } from "uuid";
import { db } from "./db.mjs";
import { groupBy } from "https://esm.sh/rxjs@7.8.1";
import { toArray } from "https://esm.sh/rxjs@7.8.1";
import { distinct } from "https://esm.sh/rxjs@7.8.1";

import { Graph, alg } from "@dagrejs/graphlib";

const schemas = (program) => {
    return of(
        z.union([
            z.object({
                type: z.literal("nodecreated"),
                data: z
                    .object({
                        id: z
                            .string()
                            .optional()
                            .transform((id) => id || uuidv4()),
                        value: z.array(z.any()).optional(),
                    })
                    .description("Data for node creation events"),
            }),
            z.object({
                type: z.literal("connectioncreated"),
                data: z
                    .object({
                        id: z
                            .string()
                            .optional()
                            .transform((id) => id || uuidv4()),
                        source: z.string(),
                        target: z.string(),
                        connect: z.string(),
                        config: z.object.passthrough(),
                    })
                    .description("Data for connection creation events"),
            }),
            z.object({
                type: z.literal("connectionremoved"),
                data: z
                    .object({
                        id: z.string(),
                    })
                    .description("Data for connection removal events"),
            }),
            z.object({
                type: z.literal("noderemoved"),
                data: z
                    .object({
                        id: z.string(),
                    })
                    .description("Data for node removal events"),
            }),
            z.object({
                type: z.literal("input"),
                data: z
                    .object({
                        id: z.string(),
                        value: z.array(z.any()),
                    })
                    .description("Data for input events"),
            }),
        ])
    );
};

const importOperator = () => pipe(switchMap((lib) => from(import(lib))));

const createConnection = (doc, source) => {
    const connect$ = doc.get$("connect").pipe(
        filter((operator) => operator !== undefined),
        importOperator(),
        map((mod) => mod.connect),
        startWith(defaultconnect),
        log(doc, "got connection operator")
    );

    const $ = connect$.pipe(
        switchMap((connect) => {
            return combineLatest(
                source.$.pipe(
                    log(doc, "got incoming thread"),
                    scan((threads, thread) => {
                        const parent = thread[thread.length - 1].state;
                        threads.set(parent, thread);
                        return threads;
                    }, new Map())
                ),
                source.$.pipe(
                    mergeMap(async (thread) => {
                        return {
                            state: await db.states.upsert({
                                id: uuidv4(),
                                parent: thread[thread.length - 1].state,
                                entity: doc.id,
                                flow: doc.flow,
                            }),
                            packets: thread
                                .map(({ packets }) => packets)
                                .flat(),
                        };
                    }),
                    connect(doc)
                )
            ).pipe(
                log(doc, "got evaluated guard"),
                mergeMap(async ([threads, { pass, evaluation }]) => {
                    const thread = threads.get(evaluation.state.parent);
                    threads.delete(evaluation.state.parent);
                    if (!pass || !thread) {
                        return null;
                    }

                    if (pass && !evaluation.packets) {
                        return thread;
                    }

                    await evaluation.state.incrementalPatch({
                        packets: evaluation.packets,
                    });

                    return thread.concat({
                        entity: doc.id,
                        state: evaluation.state.id,
                        packets: evaluation.packets,
                    });
                }),
                filter(Boolean)
            );
        })
    );

    // console.log('createConnection', doc.id);
    return {
        id: doc.id,
        source,
        $,
    };
};

export const getIO = (program) =>
    combineLatest(
        db.nodes.find({ selector: { flow: program.id } }).$,
        db.connections.find({ selector: { flow: program.id } }).$
    ).pipe(
        map(([nodes = [], connections = []]) => ({
            inputs: nodes
                .map((node) => node.id)
                .filter((id) => {
                    return !connections
                        .values()
                        .find((connection) => connection.target === id);
                }),
            outputs: nodes
                .map((node) => node.id)
                .filter((id) => {
                    return !connections
                        .values()
                        .find((connection) => connection.source === id);
                }),
        }))
    );

export const process = (program) => {
    const nodes$ = db.nodes.find({ selector: { flow: program.id } }).$.pipe(
        scan((nodes, docs) => {
            return docs.reduce((nodes, doc) => {
                const node =
                    nodes.get(doc.id) || createNode(program, doc, nodes$);

                nodes.set(node.id, node);

                return nodes;
            }, nodes);
        }, new Map())
    );

    return (input$$) => {
        const input$ = input$$.pipe(concatMap(({ packets }) => from(packets)));

        const flowInput$ = input$.pipe(
            filter(({ type }) => type === "input"),
            tap(async ({ data }) => {
                const doc = await db.nodes.findOne(data.id).exec();
                // console.log('got input', doc.id);
                doc.patch({
                    input: data.value,
                });
            }),
            ignoreElements()
        );

        const flowOutput$ = getIO(program).pipe(
            withLatestFrom(nodes$),
            switchMap(([{ outputs = [] }, nodes]) =>
                merge(...outputs.map((id) => nodes.get(id).$))
            ),
            distinctUntilChanged(deepEqual)
        );

        return merge(
            flowInput$,
            flowOutput$ /**updatePositions$, flowState$, */
        );
    };
};

const initNode = ({ flow, node, session }) => {
    const evaluations = node.collection.database.evaluations;
    const parents$ = node.get$("parents").pipe(shareReplay(1));
    const trees$ = evaluations
        .find({
            selector: {
                session,
                flow: flow.id,
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
                                    root: root.id,
                                    complete: true,
                                },
                            }).$
                    ),
                    mergeMap((sameRoot) =>
                        from(sameRoot).pipe(
                            reduce((graph, evaluation) => {
                                graph.setNode(evaluation.id, evaluation);
                                const parents = evaluation.parents || [];
                                for (const parent of parents) {
                                    graph.setEdge(parent, evaluation.id);
                                }
                            }, new Graph())
                        )
                    ),
                    scan(
                        (all, graph) => all.set(graph.sources()[0].id, graph),
                        new Map()
                    )
                )
            ),
            shareReplay(1)
        );

    const input$ = combineLatest(
        parents$.pipe(
            switchMap(
                (ids) =>
                    evaluations.find({
                        selector: {
                            session,
                            node: {
                                $in: ids,
                            },
                            complete: true,
                            consumed: false,
                        },
                        fields: ["root", "node", "id"],
                    }).$
            ),
            mergeMap((newEvaluations) => from(newEvaluations))
        ),
        trees$
    ).pipe(
        filter(([evaluation, trees]) => {
            const tree = trees.get(evaluation.root);
            return tree.node(evaluation.id);
        }),
        withLatestFrom(parents$),
        map(([evaluation, trees, parents]) => {
            const tree = trees.get(evaluation.root);
            const mst = alg.prim(tree, () => 1);
            const djk = alg
                .djikstra(mst, evaluation.id, () => 1)
                .sort((a, b) => a.distance - b.distance);

            const triggers = new Map([[evaluation.node, evaluation]]);
            for (const evalId of djk) {
                const evalNode = tree.node(evalId);
                if (
                    parents.includes(evalNode.node) &&
                    !triggers.has(evalNode.node)
                ) {
                    triggers.set(evalNode.node, evalNode);
                    if (parents.every((parent) => triggers.has(parent))) {
                        break;
                    }
                }
            }

            return [evaluation, triggers, parents];
        }),
        filter(([_, triggers, parents]) =>
            parents.every((p) => triggers.has(p))
        ),
        mergeMap(([evaluation, triggers]) =>
            Promise.all([
                evaluation.incrementalPatch({ consumed: true }),
                evaluations.upsert({
                    id: uuidv4(),
                    root: evaluation.root,
                    flow: flow.id,
                    node: node.id,
                    session,
                    parents: triggers.values().map(({ id }) => id),
                }),
            ])
        ),

        filter(([potentials, parents]) =>
            parents.every((id) => potentials.some(({ node }) => node === id))
        ),
        map(([triggeringEvals]) => triggeringEvals),
        distinct((list) => JSON.stringify(list.map(({ id }) => id).sort())),
        shareReplay(1)
    );

    const operator$ = node.get$("operator").pipe(
        filter(Boolean),
        switchMap((lib) => import(lib)),
        map((module) => module.default(node))
    );

    const output$ = operator.pipe$(
        switchMap((operator) => input$.pipe(operator))
    );

    // operator$.pipe(

    // )
    //     mergeMap((list) =>
    //         evaluations.upsert({
    //             id: uuidv4(),
    //             root: list[0].root,
    //             session,
    //             node: node.id,
    //             flow: flow.id,
    // 			parents: list.map(({ id }) => id),
    //             complete: false,
    //         })
    //     ),
    //     mergeMap(async (evaluation) => {
    // 		const input = await partial(evaluation);
    // 		return {input, evaluation}
    //     })
    // );
};

const createNode = (flow, node, nodes$) => {
    // console.log('createNode', flow.id, node.id);
    const reset$ = new Subject();
    const error$ = new ReplaySubject(1);
    const process$ = node.get$("process").pipe(
        filter((process) => process !== undefined),
        importOperator(),
        map((mod) => mod.default),
        map((process) => process(node)),
        log(node, "got process operator")
    );

    const wrapper$ = node.get$("wrapper").pipe(
        filter((wrapper) => wrapper !== undefined),
        importOperator(),
        map((mod) => mod.default),
        startWith(defaultWrapper),
        log(node, "got wrapper operator")
    );

    const connections$ = db.connections
        .find({
            selector: {
                target: {
                    $eq: node.id,
                },
            },
        })
        .$.pipe(
            tap((c) => console.log("new conns", node.id, c.length)),
            withLatestFrom(nodes$.pipe(startWith(new Map()))),
            scan((connections, [docs, nodes]) => {
                // console.log('connections for', node.id, docs.length);
                connections = new Map(connections);
                connections = docs.reduce((connections, doc) => {
                    const source = nodes.get(doc.source);
                    const connection =
                        connections.get(doc.id) ||
                        createConnection(doc, source);

                    connections.set(connection.id, connection);
                    return connections;
                }, connections);

                for (const [id] of connections.entries()) {
                    if (!docs.find((doc) => doc.id === id)) {
                        connections.delete(id);
                    }
                }

                return connections;
            }, new Map()),
            startWith(new Map()),
            distinctUntilChanged((a, b) =>
                deepEqual(Array.from(a.keys()), Array.from(b.keys()))
            ),
            shareReplay(1)
        );

    const $ = combineLatest(
        wrapper$,
        process$,
        connections$,
        reset$.pipe(startWith(true))
    ).pipe(
        switchMap(([wrapper, process, connections]) =>
            wrapper({ flow, node, process, connections })
        ),
        takeUntil(reset$),
        catchError((err) => {
            console.error(err);
            error$.next(err);
            return EMPTY;
        }),
        shareReplay(1)
    );

    return {
        id: node.id,
        doc: node,
        connections$,
        error$,
        reset$,
        $,
    };
};

const defaultWrapper = ({ flow, node, process, connections }) => {
    const wrappedInput$ = merge(
        node.get$("input").pipe(
            filter(Boolean),
            log(node, "got raw input"),
            distinctUntilChanged((a, b) => {
                const equal = deepEqual(a, b);
                return equal;
            }),
            mergeMap(async (packets) => {
                const id = uuidv4();
                const state = await db.states.upsert({
                    id,
                    flow: flow.id,
                    packets,
                    entity: node.id,
                });
                return [
                    {
                        packets,
                        state: state.id,
                    },
                ];
            })
        ),
        ...connections.values().map(({ $ }) => $)
    ).pipe(filter(Boolean), log(node, "wrapper input"), shareReplay(1));

    const state$ = wrappedInput$.pipe(
        log(node, "wrappedInput$ to state$"),
        mergeMap(
            async (thread) =>
                await db.states.upsert({
                    id: uuidv4(),
                    flow: flow.id,
                    entity: node.id,
                    parent: thread[thread.length - 1]?.state,
                    data: {},
                })
        ),
        log(node, "wrappedInput$ upserted state$"),
        shareReplay(1)
    );

    const unwrappedInput$ = zip(wrappedInput$, state$).pipe(
        log(node, "unwrappedToWrapped"),
        map(([raw, state]) => ({
            packets: raw.map(({ packets }) => packets).flat(),
            state,
        })),
        shareReplay(1)
    );

    const unwrappedOutput$ = unwrappedInput$.pipe(process);

    const wrappedOutput$ = unwrappedOutput$.pipe(
        log(node, "got unwrappedOutput"),
        mergeMap(async ({ packets, state }) => {
            state = await state.getLatest();
            state = await state.incrementalPatch({
                packets,
                complete: true,
            });

            const thread = await getThread(state);
            return thread;
        }),
        log(node, "got thread to output")
    );

    return wrappedOutput$;
};

const getThread = async (doc, rest) => {
    if (!doc.parent) {
        return [];
    }

    const parent = await doc.collection
        .findOne({
            selector: {
                id: doc.parent,
            },
        })
        .exec();

    return [].concat(await getThread(parent)).concat([
        {
            packets: doc.packets,
            state: doc.id,
        },
    ]);
};

const defaultProcess = (doc) =>
    pipe(map(() => [{ type: "random", [doc.id]: Math.random() }]));

const defaultconnect = (connection) =>
    pipe(
        log(connection, "doing default evaluation"),
        map((_evaluation) => {
            const { packets, ...evaluation } = _evaluation;
            return { pass: true, evaluation };
        })
    );

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
            }),
            map((progeny) => {
                return [doc.toJSON()].concat(progeny);
            })
        );
}

// // /* TODO:
// // - decide on a better name for program/doc
// // - rxdb methods to get env$
// // - keymirror env values for operators
// // - figure out fast, non-footgun approach to filter and wrapping for code usage.
// // - rxdb storage of invocation state and thread data
// // - Deno KV store.
// // - session key with datetime and optional namedeno
// // */
