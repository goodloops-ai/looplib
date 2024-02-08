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
} from "rxjs";
import { deepEqual } from "fast-equals";

import { z } from "zod";
import { v4 as uuidv4 } from "uuid";
import { db } from "./db.mjs";

export { db };

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

const session = new Date().toString();

export const process = (program) => {
    const nodes$ = db.nodes.find({ selector: { flow: program.id } }).$.pipe(
        scan((nodes, docs) => {
            return docs.reduce((nodes, doc) => {
                const node =
                    nodes.get(doc.id) ||
                    initNode({ flow: program, node: doc, session });

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
                await db.triggers.upsert({
                    id: uuidv4(),
                    node: data.id,
                    flow: program.id,
                    session,
                    packets: data.value,
                });
            }),
            ignoreElements()
        );

        // const flowOutput$ = getIO(program).pipe(
        //     withLatestFrom(nodes$),
        //     switchMap(([{ outputs = [] }, nodes]) =>
        //         merge(...outputs.map((id) => nodes.get(id).$))
        //     ),
        //     distinctUntilChanged(deepEqual)
        // );

        return merge(
            flowInput$
            // flowOutput$ /**updatePositions$, flowState$, */
        );
    };
};

const importModuleOrString = (node) => async (str) => {
    try {
        const mod = await import(str);
        return mod.default(node);
    } catch (e) {
        const moduleStr = [
            `import _ from 'https://esm.sh/lodash'`,
            `export default ${str}`,
        ].join("\n");
        const blob = new Blob([moduleStr], { type: "application/javascript" });
        const url = URL.createObjectURL(blob);
        const mod = await import(url);
        return pipe(
            mergeMap((trigger) =>
                of(trigger).pipe(
                    mergeMap(mod.default, 100),
                    tap((packets) => {
                        trigger.sendOutput(packets);
                    })
                )
            )
        );
    }
};

export const initNode = ({ node, session }) => {
    console.log("init node", node.id);
    const operator$ = node
        .get$("operator")
        .pipe(filter(Boolean), switchMap(importModuleOrString(node)));

    const triggers$ = node.triggers$(session).pipe(
        tap((trigger) =>
            console.log("got trigger", trigger.id, trigger.parents)
        ),
        shareReplay(1)
    );

    const output$ = operator$
        .pipe(switchMap((operator) => triggers$.pipe(operator)))
        .subscribe();
    return output$;
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
