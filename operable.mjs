import {
    BehaviorSubject,
    from,
    of,
    merge,
    pipe,
    forkJoin,
    shareReplay,
    take,
    map,
    mergeMap,
    distinct,
    concatMap,
    tap,
    switchMap,
    skip,
    finalize,
    filter,
    scan,
} from "rxjs";

import { Graph, alg } from "graphlib";
import { v4 as uuid } from "uuid";

class Operable {
    constructor(operableCore, upstreams = []) {
        this.$ = this.makeOutput$(operableCore, upstreams);
        this.upstream$ = new BehaviorSubject(upstreams);
        this.input$ = new ReplaySubject(1);
        this.destroy$ = new Subject();

        this.upstream$
            .pipe(
                switchMap((upstreams) =>
                    merge(...upstreams.map((upstream) => upstream.$))
                )
            )
            .subscribe(this.input$);
    }

    next(triggerOrPayload) {
        if (isTrigger(triggerOrPayload)) {
            this.input$.next(triggerOrPayload);
        } else {
            this.input$.next(new Trigger(triggerOrPayload));
        }
    }

    makeOutput$(operableCore) {
        let trigger$ = null;
        if (isOperator(operableCore)) {
            trigger$ = this.input$.pipe(withTriggerGraph(this, operableCore));
        } else if (isPureFunction(operableCore)) {
            trigger$ = this.input$.pipe(
                withTriggerGraph(
                    this,
                    mergeMap(async (trigger) => await operableCore(trigger))
                )
            );
        } else if (isAsyncGenerator(operableCore)) {
            let generator = operableCore();
            trigger$ = this.input$.pipe(
                withTriggerGraph(
                    this,
                    mergeMap(async (trigger) => await generator.next(trigger))
                )
            );
        } else if (isObservable(operableCore)) {
            trigger$ = operableCore.pipe(
                switchMap((emitted) => {
                    if (
                        emitted instanceof Trigger ||
                        (Array.isArray(emitted) && emitted.every(isTrigger))
                    ) {
                        return from(operableCore).pipe(
                            mergeMap((emitted) => {
                                const triggers = Array.isArray(emitted)
                                    ? emitted
                                    : [emitted];
                                return of(null).pipe(
                                    toTriggers(this, ...triggers)
                                );
                            })
                        );
                    }

                    if (isPojo(emitted) || !!emitted) {
                        return from(operableCore).pipe(toTriggers(this));
                    }

                    return this.makeOutput$(emitted);
                })
            );
        } else {
            trigger$ = from(operableCore).pipe(toTriggers(this));
        }

        return trigger$.pipe(shareReplay(1));
    }

    pipe(next, ...rest) {
        if (!next) {
            return this;
        }

        if (!next instanceof Operable) {
            next = new Operable(next);
        }

        next.upstream$
            .pipe(
                take(1),
                map((value) => Array.from(new Set(value.concat([this]))))
            )
            .subscribe(next.upstream$);

        return next.pipe(...rest);
    }
}

function operableFrom(operableCore) {
    if (operableCore instanceof Operable) {
        return operableCore;
    }

    return new Operable(operableCore);
}

function operableMerge(operables) {
    return new Operable(
        merge(...operables.map(operableFrom).map((operable) => operable.$))
    );
}

function operableCombine(
    operablesOrUpstreams,
    joinAncestor = closestSharedAncestor(operables),
    joinBeforeAncestor = false
) {
    if (Array.isArray(operablesOrUpstreams)) {
        return operableCombine(
            of(operablesOrUpstreams),
            joinAncestor,
            joinBeforeAncestor
        );
    }
    const combinationOperable = new Operable(
        operablesOrUpstreams.pipe(
            switchMap((operables) => {
                return operableMerge(operables).$.pipe(
                    map((trigger) => trigger.find(joinAncestor)),
                    mergeMap((joiningTrigger) =>
                        joinBeforeAncestor
                            ? joiningTrigger.from$
                            : of(joiningTrigger)
                    ),
                    distinct(([{ id }]) => id),
                    mergeMap((joiningTrigger) =>
                        joiningTrigger.exhaust$(operables)
                    )
                );
            })
        )
    );

    operablesOrUpstreams
        .pipe(takeUntil(combinationOperable.destroy$))
        .subscribe((operables) => {
            combinationOperable.upstream$.next(operables);
        });

    return combinationOperable;
}

function closestSharedAncestor(operables) {
    const visited = new Map();
    const queue = [...operables];

    for (const operable of operables) {
        visited.set(operable, 1);
    }

    while (queue.length > 0) {
        const current = queue.shift();
        const nextOperables = current.upstreams$.getValue();

        for (const next of nextOperables) {
            if (!visited.has(next)) {
                visited.set(next, 1);
                queue.push(next);
            } else {
                visited.set(next, visited.get(next) + 1);
            }

            if (visited.get(next) === operables.length) {
                return next;
            }
        }
    }

    return null;
}

function inSerial(operable, coreOperator, state) {
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

function inParallel(operable, coreOperator) {
    return mergeMap((inputTrigger) => {
        return of(inputTrigger).pipe(
            coreOperator,
            toTriggers(operable, inputTrigger)
        );
    });
}

function withTriggerGraph(operable, coreOperator) {
    return (input$) => {
        const source$ = input$.pipe(lockTrigger(operable));

        const firstTrigger$ = source$.pipe(
            take(1),
            inSerial(operable, coreOperator, state)
        );

        const remainingTriggers$ = source$.pipe(skip(1));

        const subsequentTriggers$ = firstTrigger$.pipe(
            switchMap((firstTrigger) =>
                remainingTriggers$.pipe(
                    firstTrigger.checkedPrevious
                        ? inSerial(state)
                        : inParallel(state)
                )
            )
        );

        return concat(firstTrigger$, subsequentTriggers$);
    };
}

function isOperator(operableCore) {
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

function toTriggers(operable, ...from) {
    return pipe(
        mergeMap((output) => {
            let output$ =
                isPojo(output) || !!output ? of(output) : from(output);

            return output$.pipe(
                map((payload) => new Trigger(payload, operable, ...from)),
                unlockTrigger(operable, ...from)
            );
        })
    );
}

function lockTrigger(operable) {
    return tap((trigger) => {
        trigger.locks$.next(trigger.locks$.getValue().concat([operable]));
    });
}

function unlockTrigger(operable, ...from) {
    return finalize(() =>
        setTimeout(
            () =>
                from.forEach((trigger) => {
                    trigger.locks$.next(
                        trigger.locks$
                            .getValue()
                            .filter((lock) => lock !== operable)
                    );
                }),
            0
        )
    );
}

class Trigger {
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
        this.fromDag$ = this.from$.pipe(createDag(this, "from$"));
        this.toDag$ = this.to$.pipe(createDag(this, "to$"));

        this.from$.next(from);
    }

    get previous() {
        this.checkedPrevious = true;
        return this._previous;
    }

    set previous(value) {
        this._previous = value;
    }

    find(query, graph = this.fromDag$.getValue()) {
        const topoSort = alg.topsort(graph);
        let result = [];

        if (isOperable(query)) {
            result = topoSort.filter(
                (nodeId) => graph.node(nodeId).operable === query
            );
        } else if (isPureFunction(query)) {
            result = topoSort.filter((nodeId) => query(graph.node(nodeId)));
        } else if (isZodSchema(query)) {
            result = topoSort
                .map((nodeId) => query.safeParse(graph.node(nodeId)))
                .filter((parseResult) => parseResult.success)
                .map((parseResult) => parseResult.data);
        } else if (query) {
            result = topoSort;
        } else {
            throw new Error(
                "Invalid query, must supply an operable, function, or Zod schema"
            );
        }

        return result.map((nodeId) => graph.node(nodeId));
    }

    sorted(graph = this.fromDag$.getValue()) {
        return alg
            .postorder(graph, graph.nodes())
            .map((nodeId) => graph.node(nodeId));
    }

    find$(query) {
        return this.fromDag$.pipe(map((dag) => this.find(query, dag)));
    }

    exhaust$(operables) {
        return this.toDag$.pipe(
            switchMap((graph) => {
                const triggers = alg
                    .postorder(graph, graph.nodes())
                    .map((nodeId) => graph.node(nodeId));

                return forkJoin(
                    triggers.map((trigger) =>
                        trigger.locks$.pipe(
                            filter((locks) => locks.length === 0)
                        )
                    )
                ).pipe(
                    map(() =>
                        triggers.filter((trigger) =>
                            operables.includes(trigger.operable)
                        )
                    ),
                    filter((triggers) => triggers.length > 0),
                    distinct((triggers) =>
                        triggers
                            .map((trigger) => trigger.id)
                            .sort()
                            .join()
                    )
                );
            })
        );
    }
}

function createDag(node, $key) {
    return pipe(
        switchMap((children) => {
            const graph = new Graph();
            graph.setNode(node.id, node);

            return from(children).pipe(
                mergeMap((child) => {
                    graph.setEdge(node.id, child.id);
                    return child[$key].pipe(createDag(child, $key));
                }),
                scan((accGraph, childGraph) => {
                    alg.postorder(childGraph, childGraph.nodes()).forEach(
                        (nodeId) => {
                            accGraph.setNode(nodeId, childGraph.node(nodeId));
                            childGraph.inEdges(nodeId).forEach(({ v, w }) => {
                                accGraph.setEdge(v, w);
                            });
                        }
                    );
                    return accGraph;
                }, graph)
            );
        })
    );
}
