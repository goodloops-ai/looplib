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
    concat,
    ReplaySubject,
    Subject,
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
        this.input$ = new ReplaySubject(1);
        this.destroy$ = new Subject();
        this.core = operableCore;

        this.upstream$
            .pipe(
                switchMap((upstreams) =>
                    merge(...upstreams.map((upstream) => upstream.$))
                )
            )
            .subscribe(this.input$);

        this.$ = this.makeOutput$(operableCore, upstreams);
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

export function inSerial(operable, coreOperator, state) {
    return concatMap((inputTrigger) => {
        console.log("INPUT TRIGGER", inputTrigger);
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
            inSerial(operable, coreOperator, state)
        );

        const remainingTriggers$ = source$.pipe(skip(1));

        const subsequentTriggers$ = firstTrigger$.pipe(
            switchMap((firstTrigger) =>
                remainingTriggers$.pipe(
                    firstTrigger.checkedPrevious
                        ? inSerial(operable, coreOperator, state)
                        : inParallel(operable, coreOperator)
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

function isTrigger(trigger) {
    return trigger instanceof Trigger;
}

function isZodSchema(operableCore) {
    return z.instanceof(operableCore);
}

function isOperable(operableCore) {
    return operableCore instanceof Operable;
}

function toTriggers(operable, ...fromTriggers) {
    return pipe(
        mergeMap((output) => {
            let output$ =
                isPojo(output) || !!output || !output
                    ? of(output)
                    : from(output);

            return output$.pipe(
                map(
                    (payload) => new Trigger(payload, operable, ...fromTriggers)
                ),
                unlockTrigger(operable, ...fromTriggers)
            );
        })
    );
}

function addToBehaviorSubject(behaviorSubject, ...values) {
    behaviorSubject.next(behaviorSubject.getValue().concat(values));
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
    console.log("UNLOCK", operable, from);
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
        this.from$.pipe(createDag(this, "from$")).subscribe(this.fromDag$);
        this.to$.pipe(createDag(this, "to$")).subscribe(this.toDag$);
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

    find(query, graph = this.fromDag$.getValue()) {
        const topoSort = alg.topsort(graph);
        let result = [];

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
                .map((nodeId) => query.safeParse(graph.node(nodeId)))
                .filter((parseResult) => parseResult.success)
                .map((parseResult) => parseResult.data);
        } else if (query) {
            result = topoSort.map((nodeId) => graph.node(nodeId));
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

    find$(query) {
        return this.fromDag$.pipe(map((dag) => this.find(query, dag)));
    }

    exhaust$(operables) {
        return this.toDag$.pipe(
            switchMap((graph) => {
                console.log("GRAPH", graph);
                const triggers = alg
                    .postorder(graph, graph.nodes())
                    .map((nodeId) => graph.node(nodeId));

                console.log("TRIGGERS", triggers.length);
                return forkJoin(
                    triggers.map((trigger) =>
                        trigger.locks$.pipe(
                            filter((locks) => locks.length === 0),
                            take(1)
                        )
                    )
                ).pipe(
                    tap((locks) => {
                        console.log("LOCKS", locks.length);
                    }),
                    map(() =>
                        triggers.filter((trigger) =>
                            operables.includes(trigger.operable)
                        )
                    ),
                    filter((triggers) => triggers.length > 0),
                    tap((locks) => {
                        console.log("Tr", locks.length);
                    }),
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

export function createDag(node, $key) {
    return pipe(
        switchMap((children) => {
            const graph = new Graph();
            graph.setNode(node.id, node);

            if (!children?.length) {
                return of(graph);
            }

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
