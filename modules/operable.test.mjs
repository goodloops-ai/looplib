import { assertEquals } from "https://deno.land/std@0.114.0/testing/asserts.ts";
import {
    createDag,
    Trigger,
    Operable,
    inSerial,
    inParallel,
} from "./operable.mjs";
import { Graph } from "graphlib";
import { v4 } from "uuid";
import { BehaviorSubject, firstValueFrom, skip, of, tap } from "rxjs";
import { z } from "zod";
import { assertEquals } from "https://deno.land/std/testing/asserts.ts";

Deno.test("inSerial should process triggers in sequence", async () => {
    const operable = new Operable(() => {});
    const state = { previous: null };
    const coreOperator = tap((inputTrigger) => {
        inputTrigger.payload += 1;
    });

    const inputTriggers = Array.from({ length: 5 }, (_, i) => {
        return { payload: i };
    });

    const result = await of(...inputTriggers)
        .pipe(inSerial(operable, coreOperator, state))
        .toPromise();

    assertEquals(result, [
        { payload: 1 },
        { payload: 2 },
        { payload: 3 },
        { payload: 4 },
        { payload: 5 },
    ]);
});

Deno.test("inParallel should process triggers concurrently", async () => {
    const operable = new Operable(() => {});
    const coreOperator = tap((inputTrigger) => {
        inputTrigger.payload += 1;
    });

    const inputTriggers = Array.from({ length: 5 }, (_, i) => {
        return { payload: i };
    });

    const result = await of(...inputTriggers)
        .pipe(inParallel(operable, coreOperator))
        .toPromise();

    assertEquals(result, [
        { payload: 1 },
        { payload: 2 },
        { payload: 3 },
        { payload: 4 },
        { payload: 5 },
    ]);
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
    assertEquals(result1[0].payload, "payload2");

    // Test find method with function
    const result2 = trigger3.find(({ payload }) => {
        console.log("payload", payload);
        return payload === "payload2";
    });
    assertEquals(result2[0].payload, "payload2");

    // Test find method with Zod schema
    const zodSchema = z.object({ payload: z.string() });
    const result3 = trigger3.find(zodSchema);
    assertEquals(
        result3.map(({ payload }) => payload),
        ["payload3", "payload2", "payload1"]
    );
});

Deno.test(
    "createDag should correctly create a directed acyclic graph",
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

        const result = await firstValueFrom(
            node.from$.pipe(createDag(node, "from$"), skip(1))
        );

        const expectedGraph = new Graph();
        expectedGraph.setNode(node.id, node);
        expectedGraph.setNode(child1.id, child1);
        expectedGraph.setNode(child2.id, child2);
        expectedGraph.setEdge(node.id, child1.id);
        expectedGraph.setEdge(node.id, child2.id);

        assertEquals(result.nodes(), expectedGraph.nodes());
        assertEquals(result.edges(), expectedGraph.edges());
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
