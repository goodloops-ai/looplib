import { assertEquals } from "https://deno.land/std@0.114.0/testing/asserts.ts";
import {
    createGraph,
    mergeGraphs,
    Trigger,
    Operable,
    inSerial,
    withTriggerGraph,
    inParallel,
    operableCombine,
    getIdealAncestor,
    // findClosestCommonAncestor,
} from "./operable.mjs";
import { Graph, alg } from "graphlib";
import { v4 } from "uuid";
import {
    BehaviorSubject,
    firstValueFrom,
    skip,
    of,
    tap,
    take,
    filter,
    mergeMap,
    map,
    pipe,
    debounceTime,
    toArray,
    from,
} from "rxjs";
import { z } from "zod";
import { deepEqual } from "fast-equals";

Deno.test("operableCombine", async () => {
    const timeouts = [];
    // Helper function to simulate async operation with random delay
    const asyncOperation = (letter) => async () => {
        const delay = Math.floor(Math.random() * 1000);

        await new Promise((resolve) => {
            timeouts.push(setTimeout(resolve, delay));
        });
        return letter + delay;
    };

    // Graph shape:
    // graph TB
    // Z --> A
    // A[A] --> B[B]
    // A --> C[C]
    // B --> D[D]
    // C --> D
    // B --> E[E]
    // C --> F[F]
    // E --> G[G]
    // F --> G
    // D --> H[H]
    const Z = new Operable(asyncOperation("Z"));
    const A = new Operable(async () => {
        await asyncOperation("A")();
        return ["A1", "A2", "A3"];
    });
    const B = new Operable(asyncOperation("B"));
    const C = new Operable(asyncOperation("C"));
    const D = new Operable(asyncOperation("D"));
    const E = new Operable(asyncOperation("E"));
    const F = new Operable(asyncOperation("F"));
    const G = new Operable(asyncOperation("G"));
    const H = new Operable(asyncOperation("H"));
    Z.id = "Z";
    A.id = "A";
    B.id = "B";
    C.id = "C";
    D.id = "D";
    E.id = "E";
    F.id = "F";
    G.id = "G";
    H.id = "H";

    Z.pipe(A);
    A.pipe(B);
    A.pipe(C);
    B.pipe(D);
    C.pipe(D);
    B.pipe(E);
    C.pipe(F);
    E.pipe(G);
    F.pipe(G);
    D.pipe(H);

    // Combine the operables D and H
    const combinedOperable = operableCombine([G, H], undefined, true);
    Z.next("trigger");

    const res = await firstValueFrom(combinedOperable.$);
    console.log("RESULT");
    assertEquals(res.from$.getValue().length, 12); // each of A's x4 for the two paths to G and H
    assertEquals(res.find(A).length, 3);
    assertEquals(res.find(A).sort(), ["A1", "A2", "A3"].sort());
    assertEquals(res.find(B).length, 3);
    assertEquals(res.find(C).length, 3);
    assertEquals(res.find(E).length, 3);
    assertEquals(res.find(D).length, 6);
    assertEquals(res.find(F).length, 3);
    assertEquals(res.find(G).length, 6);
    assertEquals(res.find(H).length, 6);
    combinedOperable.destroy();

    timeouts.forEach((timeout) => clearTimeout(timeout));
    // Assertions will be added here later
});

Deno.test("getting started", async () => {
    const start = new Operable(of({ subject: "hello world" }));

    let done = false;
    let over = false;
    start
        .pipe(
            async (trigger) => {
                const response = await (() => "ryan")();
                return { name: response };
            },
            async (trigger) => {
                const response = await (() => "33")();
                return { age: response };
            },
            (trigger) => {
                const { age } = trigger.findOne(z.object({ age: z.string() }));
                const { name } = trigger.findOne(
                    z.object({ name: z.string() })
                );
                console.log(`Hello ${name}, you are ${age} years old`);

                assertEquals(name, "ryan");
                assertEquals(age, "33");
                return true;
            },
            () => {
                done = true;
                console.log("done");
                // returning falsey should not run the next function
            },
            () => {
                over = true;
                console.log("should not run");
            }
        )
        .$.subscribe();

    await new Promise((resolve) => setTimeout(resolve, 1000));
    assertEquals(done, true);
    assertEquals(over, false);
});

Deno.test("re-using operable", async () => {
    const evens = new Operable(of(2, 4, 6, 8));
    const odds = new Operable(of(1, 3, 5, 7));
    const doubled = new Operable(
        map((trigger) => ({
            doubled: trigger.payload * 2,
        }))
    );

    doubled.$.subscribe((trigger) => {
        console.log("doubled", trigger.payload);
    });

    const resultsEven = [];
    const resultsOdd = [];

    const doubledOdds = odds
        .pipe(
            doubled,
            (trigger) => trigger.findOne(z.number()) % 2 !== 0 && trigger
        )
        .$.subscribe((trigger) => {
            resultsOdd.push(trigger);
        });

    const doubledEvens = evens
        .pipe(doubled, (trigger) => {
            console.log(
                "doubled in evens",
                trigger.payload,
                trigger.findOne(z.number()) % 2 == 0
            );
            return trigger.findOne(z.number()) % 2 == 0 && trigger;
        })
        .$.subscribe((trigger) => {
            console.log("trigger", trigger.payload);
            resultsEven.push(trigger);
        });

    await new Promise((resolve) => setTimeout(resolve, 1000));

    console.log(
        "resultsEven",
        resultsEven.map((t) => t.payload)
    );
    console.log(
        "resultsOdd",
        resultsOdd.map((t) => t.payload)
    );
    assertEquals(resultsEven.length, 4);
    assertEquals(resultsOdd.length, 4);

    doubledEvens.unsubscribe();
    doubledOdds.unsubscribe();

    resultsEven.forEach((trigger) => {
        console.log(
            "trigger",
            trigger.payload,
            trigger.find(z.number()) % 2 == 0
        );
        const num = trigger.find(z.number());

        assertEquals(trigger.payload.doubled, num * 2);
    });

    resultsOdd.forEach((trigger) => {
        const num = trigger.find(z.number());
        assertEquals(trigger.payload.doubled, num * 2);
    });
});

Deno.test(
    "Observable core emits triggers with payloads 1, 2, 3, 4, 5",
    async () => {
        const observableCore = from([1, 2, 3, 4, 5]);
        const operable = new Operable(observableCore);
        const emittedTriggers = [];

        const subscription = operable.$.subscribe({
            next: (trigger) => {
                emittedTriggers.push(trigger);
            },
        });

        // Wait for all values to be emitted
        await new Promise((resolve) => setTimeout(resolve, 100));

        // Check that there are 5 triggers
        assertEquals(emittedTriggers.length, 5);

        // Check that payloads are 1, 2, 3, 4, 5
        emittedTriggers.forEach((trigger, index) => {
            console.log("trigger", trigger.payload, index + 1);
            assertEquals(trigger instanceof Trigger, true);
            // assertEquals(trigger.payload, index + 1);
        });

        subscription.unsubscribe();
    }
);

Deno.test("async fn core", async () => {
    const operable = new Operable(async (trigger) => trigger.payload * 2);

    const subscription = operable.$.subscribe({
        next: (trigger) => {
            emittedTriggers.push(trigger);
        },
    });

    await new Promise((resolve) => setTimeout(resolve, 1000));
    const inputs = [1, 2, 3, 4, 5];
    inputs.forEach((input) => {
        operable.next(input);
    });

    const emittedTriggers = [];

    await new Promise((resolve) => setTimeout(resolve, 100));
    // Wait for all values to be emitted

    // Check that there are 5 triggers
    assertEquals(emittedTriggers.length, 5);

    // Check that payloads are 1, 2, 3, 4, 5
    emittedTriggers.forEach((trigger, index) => {
        assertEquals(trigger instanceof Trigger, true);
        assertEquals(trigger.payload, (index + 1) * 2);
    });

    subscription.unsubscribe();
});

Deno.test("operator fn core", async () => {
    const operable = new Operable(map((trigger) => trigger.payload * 2));

    const emittedTriggers = [];
    const subscription = operable.$.subscribe({
        next: (trigger) => {
            emittedTriggers.push(trigger);
        },
    });

    const inputs = [1, 2, 3, 4, 5];
    inputs.forEach((input) => {
        operable.next(input);
    });

    await new Promise((resolve) => setTimeout(resolve, 100));
    // Wait for all values to be emitted

    // Check that there are 5 triggers
    assertEquals(emittedTriggers.length, 5);

    // Check that payloads are 1, 2, 3, 4, 5
    emittedTriggers.forEach((trigger, index) => {
        assertEquals(trigger instanceof Trigger, true);
        assertEquals(trigger.payload, (index + 1) * 2);
    });

    subscription.unsubscribe();
});

Deno.test("multiOutput", async () => {
    const operable = new Operable((trigger) => [
        trigger.payload * 2,
        trigger.payload * 3,
    ]);

    const emittedTriggers = [];
    const subscription = operable.$.subscribe({
        next: (trigger) => {
            emittedTriggers.push(trigger);
        },
    });

    const inputs = [1, 2, 3, 4, 5].map((payload) => new Trigger(payload));
    inputs.forEach((input) => {
        operable.next(input);
    });

    await new Promise((resolve) => setTimeout(resolve, 100));
    // Wait for all values to be emitted

    // Check that there are 5 triggers
    assertEquals(emittedTriggers.length, 10);

    // Check that payloads are 1, 2, 3, 4, 5
    inputs.forEach((input, index) => {
        const outputPayloads = emittedTriggers
            .filter((trigger) => trigger.from$.getValue()[0].id === input.id)
            .map(({ payload }) => payload)
            .sort();
        assertEquals(
            outputPayloads.sort(),
            [input.payload * 2, input.payload * 3].sort()
        );
    });

    subscription.unsubscribe();
});

Deno.test("multiOutput operator", async () => {
    const operable = new Operable(
        mergeMap((trigger) => from([trigger.payload * 2, trigger.payload * 3]))
    );

    const emittedTriggers = [];
    const subscription = operable.$.subscribe({
        next: (trigger) => {
            emittedTriggers.push(trigger);
        },
    });

    const inputs = [1, 2, 3, 4, 5].map((payload) => new Trigger(payload));
    inputs.forEach((input) => {
        operable.next(input);
    });

    await new Promise((resolve) => setTimeout(resolve, 100));
    // Wait for all values to be emitted

    // Check that there are 5 triggers
    assertEquals(emittedTriggers.length, 10);

    // Check that payloads are 1, 2, 3, 4, 5
    inputs.forEach((input, index) => {
        const outputPayloads = emittedTriggers
            .filter((trigger) => trigger.from$.getValue()[0].id === input.id)
            .map(({ payload }) => payload)
            .sort();
        assertEquals(
            outputPayloads.sort(),
            [input.payload * 2, input.payload * 3].sort()
        );
    });

    subscription.unsubscribe();
});

Deno.test("multiOutput async generator", async () => {
    const core = async function* () {
        let trigger = { payload: 0 };
        while (true) {
            trigger = yield [trigger.payload * 2, trigger.payload * 3];
            console.log("trigger", trigger.payload);
        }
    };

    const operable = new Operable(core);

    const emittedTriggers = [];
    const subscription = operable.$.subscribe({
        next: (trigger) => {
            console.log("trigger!!!", trigger.payload);
            emittedTriggers.push(trigger);
        },
    });

    const inputs = [1, 2, 3, 4, 5].map((payload) => new Trigger(payload));
    inputs.forEach((input) => {
        operable.next(input);
    });

    await new Promise((resolve) => setTimeout(resolve, 100));
    // Wait for all values to be emitted

    // Check that there are 5 triggers
    assertEquals(emittedTriggers.length, 10);

    // Check that payloads are 1, 2, 3, 4, 5
    inputs.forEach((input, index) => {
        const outputPayloads = emittedTriggers
            .filter((trigger) => trigger.from$.getValue()[0].id === input.id)
            .map(({ payload }) => payload)
            .sort();
        assertEquals(
            outputPayloads.sort(),
            [input.payload * 2, input.payload * 3].sort()
        );
    });

    subscription.unsubscribe();
});

Deno.test("getIdealAncestors", async () => {
    // Graph shape:
    // graph TB
    // Z --> A
    // A[A] --> B[B]
    // A --> C[C]
    // B --> D[D]
    // C --> D
    // B --> E[E]
    // C --> F[F]
    // E --> G[G]
    // F --> G
    // D --> H[H]
    // G --> H
    const Z = new Operable(() => {});
    Z.id = "Z";
    const A = new Operable(() => {});
    A.id = "A";
    const B = new Operable(() => {});
    B.id = "B";
    const C = new Operable(() => {});
    C.id = "C";
    const D = new Operable(() => {});
    D.id = "D";
    const E = new Operable(() => {});
    E.id = "E";
    const F = new Operable(() => {});
    F.id = "F";
    const G = new Operable(() => {});
    G.id = "G";
    const H = new Operable(() => {});
    H.id = "H";

    Z.pipe(A);
    A.pipe(B);
    A.pipe(C);
    B.pipe(D);
    C.pipe(D);
    B.pipe(E);
    C.pipe(F);
    E.pipe(G);
    F.pipe(G);
    D.pipe(H);

    const ideal = await firstValueFrom(getIdealAncestor([G, H], "upstream$"));

    console.log("ancestors ideal", ideal);
    assertEquals(ideal, A);
    // find the first ancestor through which all previous ancestors are reachable
    // const sharedAncestors = ancestors.find(([ancestor, paths], i) => {
    //     if (!i) return false;
    //     const reachableAncestors = ancestors
    //         .slice(0, i)
    //         .map(([ancestor]) => ancestor);

    //     return reachableAncestors.every(
    //         (reachableAncestor) =>
    //             paths[reachableAncestor].distance < Infinity
    //     );
    // });

    // console.log("sharedAncestors", sharedAncestors);
});

Deno.test("withTriggerGraph: automatic serialization", async () => {
    const operable = new Operable(() => {});
    const inputTrigger = new Trigger(1, operable);
    const inputTrigger2 = new Trigger(2, operable);
    const inputTrigger3 = new Trigger(3, operable);
    const outputTriggers = [];
    const inputTriggers = [inputTrigger, inputTrigger2, inputTrigger3];

    operable.input$
        .pipe(
            withTriggerGraph(operable, pipe(map((_) => (_.previous ? 5 : 5))))
        )
        .subscribe((trigger) => {
            outputTriggers.push(trigger);
        });

    operable.next(inputTrigger);
    operable.next(inputTrigger2);
    operable.next(inputTrigger3);

    // Allow async operations to complete
    await new Promise((resolve) => setTimeout(resolve, 100));

    assertEquals(outputTriggers.length, 3);

    assertEquals(inputTrigger.checkedPrevious, true);

    outputTriggers.forEach((trigger, index) => {
        // payload set correctly
        assertEquals(trigger.payload, 5);

        // from set correctly
        assertEquals(trigger.from$.getValue()[0], inputTriggers[index]);

        // operable set correctly
        assertEquals(trigger.operable, operable);
        if (index > 0) {
            // previous set correctly
            console.log(
                "check previous",
                index,
                inputTriggers[index].previous,
                outputTriggers[index - 1].payload
            );
            assertEquals(
                inputTriggers[index].previous,
                outputTriggers[index - 1].payload
            );
        }
    });
});

Deno.test("withTriggerGraph: automatic concurrency", async () => {
    const operable = new Operable(() => {});
    const inputTrigger = new Trigger(1, operable);
    const inputTrigger2 = new Trigger(2, operable);
    const inputTrigger3 = new Trigger(3, operable);
    const outputTriggers = [];
    const inputTriggers = [inputTrigger, inputTrigger2, inputTrigger3];

    operable.input$
        .pipe(
            withTriggerGraph(
                operable,
                pipe(map((_) => 5)) // pipeline that does not check `.previous` value
            )
        )
        .subscribe((trigger) => {
            outputTriggers.push(trigger);
        });

    operable.next(inputTrigger);
    operable.next(inputTrigger2);
    operable.next(inputTrigger3);

    assertEquals(inputTrigger.checkedPrevious, false);
    // Allow async operations to complete
    await new Promise((resolve) => setTimeout(resolve, 100));

    assertEquals(outputTriggers.length, 3);

    outputTriggers.forEach((trigger, index) => {
        // payload set correctly
        assertEquals(trigger.payload, 5);

        // from set correctly
        assertEquals(trigger.from$.getValue()[0], inputTriggers[index]);

        // operable set correctly
        assertEquals(trigger.operable, operable);
        if (index > 0) {
            // previous set correctly
            assertEquals(inputTriggers[index].previous, null);
        }
        // assertEquals(inputTriggers[index].previous, null);
    });
});

Deno.test(
    "inSerial should process triggers in sequence with last state",
    async () => {
        const operable = new Operable(() => {});
        const state = { previous: null };
        const coreOperator = map((inputTrigger) => {
            return inputTrigger.payload + inputTrigger.previous || 0;
        });

        const inputTriggers = Array.from({ length: 5 }, (_, i) => {
            return new Trigger(i);
        });

        const result = await firstValueFrom(
            of(...inputTriggers).pipe(
                inSerial(operable, coreOperator, state),
                map(({ payload }) => payload),
                toArray()
            )
        );

        assertEquals(result, [0, 1, 3, 6, 10]);
    }
);

Deno.test("inParallel should process triggers concurrently", async () => {
    const operable = new Operable(() => {});
    const coreOperator = mergeMap(async (inputTrigger) => {
        await new Promise((resolve) => setTimeout(resolve, 1000));
        return inputTrigger.payload + 1;
    });

    const inputTriggers = Array.from({ length: 5 }, (_, i) => {
        return new Trigger(i);
    });

    const start = Date.now();
    const result = await firstValueFrom(
        of(...inputTriggers).pipe(
            inParallel(operable, coreOperator),
            map(({ payload }) => payload),
            toArray()
        )
    );
    const end = Date.now();

    assertEquals(result, [1, 2, 3, 4, 5]);
    assertEquals(
        end - start < 5000,
        true,
        "Execution time should be less than 5 seconds"
    );
});

Deno.test("Trigger.find method", async () => {
    // Create Operables
    const operable1 = new Operable(() => {});
    const operable2 = new Operable(() => {});
    const operable3 = new Operable(() => {});

    // Create Triggers
    const trigger1 = new Trigger("payload1", operable1);
    const trigger2 = new Trigger("payload2", operable2, trigger1);
    const trigger3 = new Trigger("payload3", operable3, trigger2);

    // Test find method with Operable
    const result1 = trigger3.find(operable2);
    assertEquals(result1[0], "payload2");

    // Test find method with function
    const result2 = trigger3.find((payload) => {
        console.log("payload", payload);
        return payload === "payload2";
    });
    assertEquals(result2[0], "payload2");

    // Test find method with Zod schema
    const zodSchema = z.string();
    const result3 = trigger3.find(zodSchema);
    assertEquals(result3, ["payload3", "payload2", "payload1"]);
});

Deno.test("createGraph should correctly create a cyclic graph", async () => {
    // Graph shape:
    // graph TB
    // A[A] --> B[B]
    // B --> C[C]
    // C --> A

    const nodeA = {
        id: "A",
        from$: new BehaviorSubject([]),
    };

    const nodeB = {
        id: "B",
        from$: new BehaviorSubject([]),
    };

    const nodeC = {
        id: "C",
        from$: new BehaviorSubject([]),
    };

    nodeA.from$.next([nodeB]);
    nodeB.from$.next([nodeC]);
    nodeC.from$.next([nodeA]);

    let result;
    createGraph(nodeA, "from$").subscribe((_graph) => {
        console.log("graph", _graph);
        result = _graph;
    });

    await new Promise((resolve) => setTimeout(resolve, 200));

    const expectedGraph = new Graph();
    expectedGraph.setNode(nodeA.id, nodeA);
    expectedGraph.setNode(nodeB.id, nodeB);
    expectedGraph.setNode(nodeC.id, nodeC);
    expectedGraph.setEdge(nodeA.id, nodeB.id);
    expectedGraph.setEdge(nodeB.id, nodeC.id);
    expectedGraph.setEdge(nodeC.id, nodeA.id);

    const sortEdges = (a, b) => {
        if (a.v === b.v) {
            return a.w.localeCompare(b.w);
        }
        return a.v.localeCompare(b.v);
    };

    const resultEdgesSorted = result.edges().sort(sortEdges);
    const expectedEdgesSorted = expectedGraph.edges().sort(sortEdges);

    // Now compare the sorted edges
    assertEquals(resultEdgesSorted, expectedEdgesSorted);

    result.nodes().forEach((node) => {
        assertEquals(result.node(node), expectedGraph.node(node));
    });
    result.edges().forEach((edge) => {
        assertEquals(result.edge(edge), expectedGraph.edge(edge));
    });
});

Deno.test(
    "createGraph should correctly create a directed acyclic graph",
    async () => {
        const node = {
            id: v4(),
            from$: new BehaviorSubject([]),
        };

        const child1 = {
            id: v4(),
            from$: new BehaviorSubject([]),
        };

        const child2 = {
            id: v4(),
            from$: new BehaviorSubject([]),
        };
        node.from$.next([child1, child2]);

        const result = await firstValueFrom(createGraph(node, "from$"));

        const expectedGraph = new Graph();
        expectedGraph.setNode(node.id, node);
        expectedGraph.setNode(child1.id, child1);
        expectedGraph.setNode(child2.id, child2);
        expectedGraph.setEdge(node.id, child1.id);
        expectedGraph.setEdge(node.id, child2.id);

        const sortEdges = (a, b) => {
            if (a.v === b.v) {
                return a.w.localeCompare(b.w);
            }
            return a.v.localeCompare(b.v);
        };

        const resultEdgesSorted = result.edges().sort(sortEdges);
        const expectedEdgesSorted = expectedGraph.edges().sort(sortEdges);

        // Now compare the sorted edges
        assertEquals(resultEdgesSorted, expectedEdgesSorted);

        assertEquals(result.nodes(), expectedGraph.nodes());
        result.nodes().forEach((node) => {
            assertEquals(result.node(node), expectedGraph.node(node));
        });
        result.edges().forEach((edge) => {
            assertEquals(result.edge(edge), expectedGraph.edge(edge));
        });
    }
);

Deno.test(
    "Trigger.exhaust$ does not emit until all locks are released",
    async () => {
        // Create Operables
        const operable1 = new Operable(() => {});
        const operable2 = new Operable(() => {});

        // Create Triggers
        const trigger1 = new Trigger("payload1", operable1);
        const trigger2 = new Trigger("payload2", operable2, trigger1);

        // Set a lock on trigger2
        trigger2.locks$.next([operable1]);

        // Set up a promise that will resolve when exhaust$ emits
        let resolveExhaust;
        const exhaustPromise = new Promise((resolve) => {
            resolveExhaust = resolve;
        });
        trigger1.exhaust$([operable1, operable2]).subscribe(resolveExhaust);

        // exhaust$ should not emit yet because there's a lock
        let exhaustEmitted = false;
        exhaustPromise.then(() => {
            exhaustEmitted = true;
        });
        await new Promise((resolve) => setTimeout(resolve, 1000)); // wait for 1 second
        assertEquals(exhaustEmitted, false);

        // Release the lock
        trigger2.locks$.next([]);

        // Now exhaust$ should emit
        await exhaustPromise;
        assertEquals(exhaustEmitted, true);
    }
);

Deno.test(
    "Trigger.exhaust$ emits correct payload set with complex graph",
    async () => {
        // Create Operables
        const operable1 = new Operable(() => {});
        const operable2 = new Operable(() => {});
        const operable3 = new Operable(() => {});
        const operable4 = new Operable(() => {});

        // Create Triggers
        const trigger1 = new Trigger("payload1", operable1);
        const trigger2 = new Trigger("payload2", operable2, trigger1);
        const trigger3 = new Trigger("payload3", operable3, trigger2);
        const trigger4 = new Trigger("payload4", operable4, trigger3);

        // Set a lock on trigger4
        trigger4.locks$.next([operable3]);

        // Set up a promise that will resolve when exhaust$ emits
        let resolveExhaust;
        const exhaustPromise = new Promise((resolve) => {
            console.log("exhaustPromise created");
            resolveExhaust = resolve;
        });
        trigger1
            .exhaust$([operable2, operable3, operable4])
            .subscribe(resolveExhaust);

        // exhaust$ should not emit yet because there's a lock
        let exhaustEmitted = false;
        exhaustPromise.then(() => {
            console.log("exhaustPromise resolved");
            exhaustEmitted = true;
        });
        await new Promise((resolve) => setTimeout(resolve, 1000)); // wait for 1 second
        assertEquals(exhaustEmitted, false);

        // console.log("trigger4.locks$.value", trigger4.locks$.value);
        // Release the lock
        trigger4.locks$.next([]);

        // Now exhaust$ should emit
        const result = await exhaustPromise;
        console.log("exhaustPromise result", exhaustEmitted, result.length);
        assertEquals(exhaustEmitted, true);

        // Check the payload set
        const payloadSet = result;
        assertEquals(payloadSet.map(({ payload }) => payload).sort(), [
            "payload2",
            "payload3",
            "payload4",
        ]);
    }
);

Deno.test("Trigger (de)serialization", async () => {
    // Create a complex DAG of triggers
    const trigger1 = new Trigger("payload1", "operable1");
    const trigger2 = new Trigger("payload2", "operable2", trigger1);
    const trigger3 = new Trigger("payload3", "operable3", trigger1, trigger2);

    // Serialize the DAG
    const serializedDag = await firstValueFrom(
        trigger1.toJson$().pipe(take(1))
    );

    // Deserialize the DAG
    const shouldBe1 = Trigger.deserializeGraph(serializedDag);

    // Check if the deserialized DAG matches the original DAG
    assertEquals(shouldBe1.id, trigger1.id);
    assertEquals(shouldBe1.payload, trigger1.payload);
    assertEquals(shouldBe1.operable, trigger1.operable);
    assertEquals(
        shouldBe1.to$.getValue().map((t) => t.id),
        [trigger2.id, trigger3.id]
    );
});

Deno.test("Operable caching behavior", async () => {
    const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
    const asyncFn = async (trigger) => {
        // await delay(1000);
        console.log("EXEC", trigger.id);
        return trigger.payload + 1;
    };

    const operable1 = new Operable(asyncFn);
    const operable2 = new Operable(asyncFn);
    const operable3 = new Operable(asyncFn);

    operable1.pipe(operable2, operable3);

    let start = Date.now();
    const trigger = new Trigger(0, operable1);
    operable1.next(trigger);

    const result = await firstValueFrom(operable3.$);

    let end = Date.now();
    let timeDifference = end - start;

    console.log(`OG Time difference: ${timeDifference} ms`);
    assertEquals(result.payload, 3);

    // Serialize the trigger
    const serializedTrigger = await firstValueFrom(trigger.toJson$());

    // Deserialize the trigger
    const deserializedTrigger = Trigger.deserializeGraph(serializedTrigger);

    // Create a new identical pipeline
    const newOperable1 = new Operable(asyncFn);
    newOperable1.id = operable1.id;
    const newOperable2 = new Operable(asyncFn);
    newOperable2.id = operable2.id;
    const newOperable3 = new Operable(asyncFn);
    newOperable3.id = operable3.id;

    newOperable1.pipe(newOperable2, newOperable3);
    console.log(deserializedTrigger.id, trigger.id);
    console.log(
        deserializedTrigger.to$.getValue().map((t) => t.operable),
        newOperable1.id
    );
    start = Date.now();
    newOperable1.next(deserializedTrigger);
    const newResult = await firstValueFrom(newOperable3.$);
    end = Date.now();
    timeDifference = end - start;

    // console.log(newResult, result);
    assertEquals(newResult.payload, 3);
    assertEquals(newResult.id, result.id);
    console.log("PASS");
});
