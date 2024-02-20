import {
    BehaviorSubject,
    from,
    of,
    merge,
    pipe,
    forkJoin,
    shareReplay,
    take,
    EMPTY,
    map,
    mergeMap,
    delay,
    distinct,
    concatMap,
    tap,
    switchMap,
    skip,
    finalize,
    filter,
    scan,
    takeUntil,
    concat,
    ReplaySubject,
    combineLatest,
    debounceTime,
    zip,
    Subject,
    isObservable,
    asyncScheduler,
    observeOn,
    distinctUntilChanged,
} from "rxjs";

import { Graph, alg } from "graphlib";
import { v4 as uuid } from "uuid";
import { z } from "zod";
import isPojo from "is-pojo";

export class ConfigurableOperator {
    constructor(operatorFactory, config) {
        this.config$ = new BehaviorSubject(config);
        this.operatorFactory = operatorFactory;
        this.$ = this.config$.pipe(
            switchMap((config) => this.operatorFactory(config))
        );
    }

    configure(config) {
        this.config$.next(config);
    }
}

export class Operable {
    constructor(operableCore, upstreams = []) {
        this.id = uuid();
        this.upstream$ = new BehaviorSubject(upstreams);
        this.downstream$ = new BehaviorSubject([]);
        this.input$ = new ReplaySubject(10);
        this.destroy$ = new Subject();
        this.core = operableCore;

        this.upstream$
            .pipe(
                switchMap((upstreams) =>
                    merge(...upstreams.map((upstream) => upstream.$))
                ),
                distinct((trigger) => trigger.id)
            )
            .subscribe(this.input$);

        this.$ = this.makeOutput$(operableCore, upstreams);

        // this.$.pipe(takeUntil(this.destroy$)).subscribe();
    }

    next(triggerOrPayload) {
        if (isTrigger(triggerOrPayload)) {
            this.input$.next(triggerOrPayload);
        } else {
            this.input$.next(new Trigger(triggerOrPayload));
        }
    }

    withCachedOutput(pipeline) {
        const cachePipeline$ = this.input$.pipe(
            filter((trigger) =>
                trigger.to$.getValue().some((t) => t.operable === this.id)
            ),
            mergeMap((trigger) => {
                const cached = trigger.to$
                    .getValue()
                    .filter((t) => t.operable === this.id)
                    .map((t) => {
                        t.operable = this;
                        return t;
                    });
                return from(cached);
            })
        );

        const realPipeline$ = this.input$.pipe(
            filter(
                (trigger) =>
                    !trigger.to$.getValue().some((t) => t.operable === this.id)
            ),
            pipeline
        );

        return merge(cachePipeline$, realPipeline$);
    }

    makeOutput$(operableCore) {
        let trigger$ = null;
        if (isOperator(operableCore)) {
            // console.log("OPERATOR");
            trigger$ = this.withCachedOutput(
                withTriggerGraph(this, operableCore)
            );
        } else if (isAsyncGenerator(operableCore)) {
            // console.log("ASYNC GENERATOR");
            let generator = operableCore();
            generator.next();
            trigger$ = this.withCachedOutput(
                withTriggerGraph(
                    this,
                    mergeMap(async (trigger) => {
                        // console.log("ASYNC GENERATOR");
                        return (await generator.next(trigger)).value;
                    })
                )
            );
        } else if (isPureFunction(operableCore)) {
            trigger$ = this.withCachedOutput(
                withTriggerGraph(
                    this,
                    mergeMap(async (trigger) => await operableCore(trigger))
                )
            );
        } else if (isObservable(operableCore)) {
            const core = operableCore;
            trigger$ = core.pipe(
                take(1),
                switchMap((emitted) => {
                    if (
                        emitted instanceof Trigger ||
                        (Array.isArray(emitted) && emitted.every(isTrigger))
                    ) {
                        return from(core).pipe(
                            mergeMap((emitted) => {
                                const triggers = Array.isArray(emitted)
                                    ? emitted
                                    : [emitted];

                                return of(Trigger).pipe(
                                    toTriggers(this, ...triggers)
                                );
                            })
                        );
                    }

                    if (isPojo(emitted) || !!emitted) {
                        return from(core).pipe(toTriggers(this));
                    }

                    return core.pipe(
                        switchMap((newCore) => this.makeOutput$(newCore))
                    );
                })
            );
        } else if (operableCore instanceof Trigger || isPojo(operableCore)) {
            trigger$ = of(operableCore).pipe(toTriggers(this));
        } else {
            trigger$ = from(operableCore).pipe(toTriggers(this));
        }

        return trigger$.pipe(delay(0), shareReplay(1));
    }

    pipe(next, ...rest) {
        if (!next) {
            return this;
        }

        if (!(next instanceof Operable)) {
            next = new Operable(next);
        }

        // console.log("PIPE", next);
        addToBehaviorSubject(this.downstream$, next);
        addToBehaviorSubject(next.upstream$, this);

        return next.pipe(...rest);
    }

    unpipe(next) {
        if (!next) {
            return this;
        }

        removeFromBehaviorSubject(this.upstream$, next);
        removeFromBehaviorSubject(next.downstream$, this);

        return this;
    }

    flow(fn, fromParentTrigger) {
        const operables = fn(this);
        return operableCombine(operables, this, fromParentTrigger);
    }

    destroy() {
        this.destroy$.next();
    }

    toString() {
        return this.id;
    }
}

export function operableFrom(operableCore) {
    if (operableCore instanceof Operable) {
        return operableCore;
    }

    return new Operable(operableCore);
}

export function operableMerge(operables) {
    return new Operable(
        merge(...operables.map(operableFrom).map((operable) => operable.$))
    );
}

export function operableCombine(
    operables$ = new BehaviorSubject([]),
    joinAncestor,
    joinBeforeAncestor = false
) {
    if (Array.isArray(operables$)) {
        const upstreams$ = new BehaviorSubject(operables$);
        return operableCombine(upstreams$, joinAncestor, joinBeforeAncestor);
    }

    const operablesWithAncestor$ = operables$.pipe(
        switchMap((operables) => {
            if (joinAncestor) {
                return of({ operables, ancestor: joinAncestor });
            }

            return getIdealAncestor(operables, "upstream$").pipe(
                map((ancestor) => ({ operables, ancestor }))
            );
        })
    );

    const cancel$ = new Subject();

    const combinationOperable = new Operable(
        operablesWithAncestor$.pipe(
            switchMap(({ operables, ancestor }) => {
                return merge(...operables.map(({ $ }) => $)).pipe(
                    map((trigger) => trigger.findTriggers(ancestor)[0]),
                    mergeMap((joiningTrigger) =>
                        joinBeforeAncestor
                            ? joiningTrigger.from$.pipe(
                                  mergeMap((_from) => from(_from))
                              )
                            : of(joiningTrigger)
                    ),
                    distinct(({ id }) => id),
                    mergeMap((joiningTrigger) => {
                        return joiningTrigger.exhaust$(operables, cancel$);
                    }),
                    switchMap((triggers) => {
                        const notFound = operables.filter(
                            (operable) =>
                                !triggers.some(
                                    (trigger) => trigger.operable === operable
                                )
                        );

                        return zip(
                            of(triggers),
                            ...notFound.map((operable) =>
                                operable.$.pipe(take(1))
                            )
                        ).pipe(take(1));
                    }),
                    map(([triggers, ...rest]) => triggers.concat(rest))
                );
            })
        )
    );

    combinationOperable.destroy$.subscribe(cancel$);

    combinationOperable.upstream$.next(operables$.getValue());

    combinationOperable.upstream$
        .pipe(distinctUntilChanged(), takeUntil(combinationOperable.destroy$))
        .subscribe(operables$);

    return combinationOperable;
}

export function closestSharedAncestor(operables) {
    const visited = new Map();
    const queue = [...operables];
    let rootAncestor = null;

    for (const operable of operables) {
        visited.set(operable, 1);
    }

    while (queue.length > 0) {
        const current = queue.shift();
        const nextOperables = current.upstream$.getValue();

        for (const next of nextOperables) {
            if (!visited.has(next)) {
                visited.set(next, 1);
                queue.push(next);
            } else {
                visited.set(next, visited.get(next) + 1);
            }
            if (visited.get(next) === operables.length) {
                rootAncestor = next;
            }
        }
    }

    return rootAncestor;
}

export function inSerial(operable, coreOperator, state) {
    return concatMap((inputTrigger) => {
        inputTrigger.previous = state.previous;
        return of(inputTrigger).pipe(
            coreOperator,
            tap((output) => {
                state.previous = output;
            }),
            toTriggers(operable, inputTrigger)
        );
    });
}

export function inParallel(operable, coreOperator) {
    return mergeMap((inputTrigger) => {
        return of(inputTrigger).pipe(
            coreOperator,
            toTriggers(operable, inputTrigger)
        );
    });
}

export function withTriggerGraph(operable, coreOperator) {
    return (input$) => {
        const state = { previous: null };
        const source$ = input$.pipe(lockTrigger(operable));

        const firstTrigger$ = source$.pipe(
            take(1),
            inSerial(operable, coreOperator, state),
            shareReplay(1)
        );

        const firstTriggerDone$ = source$.pipe(
            take(1),
            switchMap((trigger) =>
                trigger.locks$.pipe(
                    filter((locks) => !locks.some((lock) => lock === operable)),
                    map(() => trigger)
                )
            ),
            take(1)
        );

        const remainingTriggers$ = source$.pipe(skip(1));

        const subsequentTriggers$ = zip(
            merge(firstTrigger$, firstTriggerDone$),
            source$
        ).pipe(
            take(1),
            switchMap(([_, firstInputTrigger]) => {
                const nextOperator = firstInputTrigger?.checkedPrevious
                    ? inSerial
                    : inParallel;

                return remainingTriggers$.pipe(
                    nextOperator(operable, coreOperator, state)
                );
            })
        );

        return merge(firstTrigger$, subsequentTriggers$);
    };
}

function isOperator(operableCore) {
    // console.log("IS OPERATOR", operableCore.toString());
    return (
        operableCore.toString() ===
        `e=>{if(xr(e))return e.lift(function(t){try{return r(t,this)}catch(o){this.error(o)}});throw new TypeError("Unable to lift unknown Observable type")}`
    );
}

function isPureFunction(operableCore) {
    return (
        typeof operableCore === "function" &&
        (!operableCore.prototype || !operableCore.prototype.constructor.name)
    );
}

function isAsyncGenerator(operableCore) {
    return (
        operableCore &&
        operableCore.constructor &&
        operableCore.constructor.name === "AsyncGeneratorFunction"
    );
}

function isTrigger(trigger) {
    return trigger instanceof Trigger;
}

function isZodSchema(operableCore) {
    return !!operableCore.safeParse;
}

function isOperable(operableCore) {
    return operableCore instanceof Operable;
}

function toTriggers(operable, ...fromTriggers) {
    return pipe(
        mergeMap((output) => {
            if (output === null || output === undefined || output === false) {
                output = EMPTY;
            } else if (output instanceof Trigger) {
                return of(output).pipe(
                    unlockTrigger(operable, ...fromTriggers)
                );
            } else if (
                (isPojo(output) || !!output || !output) &&
                !Array.isArray(output)
            ) {
                output = of(output);
            } else if (output === Trigger) {
                output = of(null);
            } else {
                output = from(output);
            }

            return output.pipe(
                map(
                    (payload) => new Trigger(payload, operable, ...fromTriggers)
                ),
                unlockTrigger(operable, ...fromTriggers)
            );
        })
    );
}

function addToBehaviorSubject(behaviorSubject, ...values) {
    behaviorSubject.next(
        Array.from(new Set(behaviorSubject.getValue().concat(values)))
    );
}

function removeFromBehaviorSubject(behaviorSubject, ...values) {
    behaviorSubject.next(
        behaviorSubject.getValue().filter((value) => !values.includes(value))
    );
}

function lockTrigger(operable) {
    return tap((trigger) => {
        addToBehaviorSubject(trigger.locks$, operable);
    });
}

function unlockTrigger(operable, ...from) {
    // console.log("UNLOCK", operable, from);
    return finalize(() =>
        setTimeout(
            () =>
                from.forEach((trigger) => {
                    removeFromBehaviorSubject(trigger.locks$, operable);
                }),
            0
        )
    );
}

export class Trigger {
    _previous = null;
    checkedPrevious = false;

    locks$ = new BehaviorSubject([]);
    from$ = new BehaviorSubject([]);
    fromDag$ = new BehaviorSubject([]); // BehaviorSubject for the DAG
    toDag$ = new BehaviorSubject([]); // BehaviorSubject for the DAG
    to$ = new BehaviorSubject([]);

    constructor(payload, operable, ...from) {
        this.id = uuid(); // unique identifier flag
        if (Array.is) this.payload = payload;
        this.operable = operable;
        createGraph(this, "from$").subscribe(this.fromDag$);
        createGraph(this, "to$").subscribe(this.toDag$);
        this.payload = payload;
        addToBehaviorSubject(this.from$, ...from);
        from.forEach((trigger) => addToBehaviorSubject(trigger.to$, this));
    }

    get previous() {
        this.checkedPrevious = true;
        return this._previous;
    }

    set previous(value) {
        this._previous = value;
    }

    findOne(query) {
        return this.find(query)[0];
    }

    find(query, graph = this.fromDag$.getValue()) {
        if (isPureFunction(query)) {
            const _query = query;
            query = (node) => _query(node.payload);
        } else if (isZodSchema(query)) {
            const schema = query;
            query = (node) => schema.safeParse(node.payload).success;
        }

        return this.findTriggers(query, graph).map((node) => node.payload);
    }

    findTriggers(query, graph = this.fromDag$.getValue()) {
        const topoSort = alg.topsort(graph);
        let result = [];

        // console.log("QUERY", query);
        // console.log(
        //     "TOPO SORT",
        //     topoSort.map((nodeId) => graph.node(nodeId).payload)
        // );

        if (isOperable(query)) {
            result = topoSort
                .filter((nodeId) => graph.node(nodeId).operable === query)
                .map((nodeId) => graph.node(nodeId));
        } else if (isPureFunction(query)) {
            result = topoSort
                .map((nodeId) => graph.node(nodeId))
                .filter((node) => query(node));
        } else if (isZodSchema(query)) {
            result = topoSort
                .map((nodeId) => graph.node(nodeId))
                .filter((node) => query.safeParse(node).success);
        } else if (query) {
            result = topoSort;
        } else {
            throw new Error(
                "Invalid query, must supply an operable, function, or Zod schema"
            );
        }

        return result;
    }

    sorted(graph = this.fromDag$.getValue()) {
        return alg
            .postorder(graph, graph.nodes())
            .map((nodeId) => graph.node(nodeId));
    }
    exhaust$(operables, cancel$ = EMPTY) {
        return this.toDag$.pipe(
            debounceTime(1000),
            switchMap((graph) => {
                const triggers = alg
                    .postorder(graph, graph.nodes())
                    .map((nodeId) => graph.node(nodeId));

                // return of(triggers.slice(0, 1));
                const toJoin = triggers.slice(0, 10).map((trigger) =>
                    trigger.locks$.pipe(
                        filter((locks) => locks.length === 0),
                        map(() => trigger),
                        take(1)
                    )
                );

                const nonce = Math.random();
                return forkJoin(toJoin).pipe(
                    map(() =>
                        triggers.filter((trigger) =>
                            operables.includes(trigger.operable)
                        )
                    ),
                    filter((triggers) => triggers.length > 0),
                    distinct(
                        (triggers) =>
                            triggers
                                .map((trigger) => trigger.id)
                                .sort()
                                .join(),
                        cancel$
                    )
                );
            }),
            takeUntil(cancel$)
        );
    }

    serialize() {
        return {
            id: this.id,
            payload: this.payload,
            operable: this.operable.toString(),
            from: this.from$.getValue().map((trigger) => trigger.id),
            to: this.to$.getValue().map((trigger) => trigger.id),
        };
    }

    toJson$() {
        return this.toDag$.pipe(
            map((graph) => {
                return JSON.stringify(
                    alg
                        .postorder(graph, graph.nodes())
                        .map((nodeId) => graph.node(nodeId).serialize())
                );
            })
        );
    }

    static deserializeGraph(str) {
        const json = JSON.parse(str);
        const graph = new Graph();

        for (const triggerJson of json) {
            graph.setNode(triggerJson.id, triggerJson);
            triggerJson.to.forEach((toId) => {
                graph.setEdge(triggerJson.id, toId);
            });
        }

        const source = graph.sources()[0];

        return this.deserialize(graph, source);
    }

    static deserialize(graph, id) {
        const triggerJson = graph.node(id);
        const trigger = new Trigger(triggerJson.payload, triggerJson.operable);
        trigger.id = triggerJson.id;

        graph.setNode(triggerJson.id, trigger);

        trigger.from$.next(
            triggerJson.from.map((fromId) => {
                graph.node(fromId) instanceof Trigger
                    ? graph.node(fromId)
                    : Trigger.deserialize(graph, fromId);
            })
        );
        trigger.to$.next(
            triggerJson.to.map((toId) =>
                graph.node(toId) instanceof Trigger
                    ? graph.node(toId)
                    : Trigger.deserialize(graph, toId)
            )
        );

        return trigger;
    }

    static fromJson(json) {}
}

export function createGraph(node, $key, reverse = false, visited = new Set()) {
    if (Array.isArray(node)) {
        return createGraphs(node, $key, reverse, visited).pipe(
            map((graphs) => mergeGraphs(new Graph(), ...graphs))
        );
    }

    // Base case: if we've already visited this node, return an empty graph to avoid cycles
    if (visited.has(node.id)) {
        return of(new Graph());
    }

    // Mark this node as visited
    visited.add(node.id);

    return node[$key].pipe(
        // Use switchMap to handle unsubscribing from previous children observables
        switchMap((children) => {
            // If there are no children, return a graph with just the current node
            if (!children || children.length === 0) {
                let singleNodeGraph = new Graph();
                singleNodeGraph.setNode(node.id, node);
                return of(singleNodeGraph);
            }

            // Otherwise, recursively create graphs for each child
            return createGraphs(children, $key, reverse, visited).pipe(
                // Combine all child graphs with the current node into a new graph
                map((childGraphs) => {
                    let combinedGraph = new Graph();
                    combinedGraph.setNode(node.id, node);

                    mergeGraphs(combinedGraph, ...childGraphs);
                    // Connect the current node to its children
                    children.forEach((child) => {
                        if (reverse) combinedGraph.setEdge(child.id, node.id);
                        else combinedGraph.setEdge(node.id, child.id);
                    });

                    return combinedGraph;
                })
            );
        }),
        // Ensure that the graph is shared and replayed to new subscribers
        shareReplay(1)
    );
}

export function createGraphs(nodes, $key, reverse, visited) {
    return combineLatest(
        nodes.map((node) => createGraph(node, $key, reverse, new Set(visited)))
    );
}

export function mergeGraphs(target, ...graphs) {
    graphs.forEach((graph) => {
        graph.nodes().forEach((nodeId) => {
            target.setNode(nodeId, target.node(nodeId) || graph.node(nodeId));
        });

        graph.edges().forEach((edge) => {
            target.setEdge(edge.v, edge.w);
        });
    });

    return target;
}

function findAllAncestors(graph, startNode) {
    let ancestors = new Set();
    let visited = new Set();
    let stack = [startNode];

    while (stack.length > 0) {
        let currentNode = stack.pop();
        if (!visited.has(currentNode)) {
            visited.add(currentNode);
            let predecessors = graph.predecessors(currentNode);
            if (predecessors) {
                predecessors.forEach((pred) => {
                    ancestors.add(pred);
                    stack.push(pred);
                });
            }
        }
    }

    return ancestors;
}

export function findClosestCommonAncestor(graph, nodes) {
    let allAncestors = nodes.map((node) => findAllAncestors(graph, node));
    let commonAncestors = allAncestors.reduce((acc, ancestors) => {
        return new Set([...acc].filter((x) => ancestors.has(x)));
    });

    // console.log("COMMON ANCESTORS", commonAncestors);

    let closestAncestor = null;
    let closestDistance = Infinity;
    commonAncestors.forEach((ancestor) => {
        let isAncestorOfAll = allAncestors.every((ancestors) =>
            ancestors.has(ancestor)
        );
        if (isAncestorOfAll) {
            let distance = alg.dijkstra(
                graph,
                ancestor,
                null,
                graph.nodeEdges.bind(graph)
            );
            let minDistance = Math.min(
                ...nodes.map((node) => distance[node].distance)
            );
            if (minDistance < closestDistance) {
                closestDistance = minDistance;
                closestAncestor = ancestor;
            }
        }
    });

    // Verify that the closest ancestor is not bypassed by any other common ancestor
    if (closestAncestor) {
        let bypassed = Array.from(commonAncestors).some((ancestor) => {
            return (
                ancestor !== closestAncestor &&
                allAncestors.some((ancestors) => ancestors.has(ancestor))
            );
        });

        if (bypassed) {
            return null;
        }
    }

    return closestAncestor;
}

function getIncomingEdges(graph, node) {
    return graph.inEdges(node) || [];
}

export function getIdealAncestor(_query, $key, reverse = true) {
    return createGraph(_query, $key, reverse).pipe(
        map((graph) => {
            const query = _query.map((node) => node.id);
            const paths = alg.dijkstraAll(graph);
            // console.log("PATHS", paths);

            // find all nodes that are reachable by all query nodes
            const queryPaths = query.map((node) => paths[node]);
            const ancestors = Object.entries(paths)
                .filter(([_, paths]) =>
                    query.every((id) => paths[id].distance < Infinity)
                )
                .sort((a, b) => {
                    // the sum of distances to all query nodes
                    const aDistance = query.reduce(
                        (acc, id) => acc + a[1][id].distance,
                        0
                    );
                    const bDistance = query.reduce(
                        (acc, id) => acc + b[1][id].distance,
                        0
                    );
                    return aDistance - bDistance;
                });

            // console.log("REACHABLE BY ALL", ancestors);
            // return first ancestor that can reach all prior ancestors and is reached by all subsequent ancestors

            const res = ancestors.find(([ancestor, paths], index) => {
                const priorAncestors = ancestors.slice(0, index);
                const subsequentAncestors = ancestors.slice(index + 1);
                const canReachAllPrior = priorAncestors.every(
                    ([a, _]) => paths[a].distance < Infinity
                );
                const isReachedByAllSubsequent = subsequentAncestors.every(
                    ([a, _]) => _[ancestor].distance < Infinity
                );
                return canReachAllPrior && isReachedByAllSubsequent;
            });

            if (!res) {
                return null;
            }

            return graph.node(res[0]);
        })
    );
}