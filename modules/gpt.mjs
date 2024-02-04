import { z } from "zod";
import { OpenAI } from "openai";
import {
    combineLatest,
    map,
    switchMap,
    catchError,
    concatMap,
    from,
    withLatestFrom,
    startWith,
    fromEvent,
    filter,
    concat,
    take,
    takeUntil,
    buffer,
    of,
    zip,
    tap,
    mergeMap,
    pipe,
    shareReplay,
    distinctUntilChanged,
    EMPTY,
    firstValueFrom,
} from "rxjs";
import { deepEqual } from "fast-equals";

export const OPENAI_API_KEY = "OPENAI_API_KEY";
export const ENV = [OPENAI_API_KEY];

export const schemas = (program) =>
    program.collection.database.getLocal$("ENV").pipe(
        filter(Boolean),
        switchMap((env) => env.get$(OPENAI_API_KEY)),
        filter(Boolean),
        startWith(OPENAI_API_KEY),
        map((OPENAI_API_KEY) =>
            z.union([
                z.object({
                    type: z.literal("config"),
                    data: z.object({
                        prompt: z.string().optional(),
                        role: z
                            .enum(["user", "assistant", "system"])
                            .default("user"),
                        temperature: z
                            .number()
                            .min(0)
                            .max(1)
                            .step(0.1)
                            .default(0.3),
                        model: z
                            .enum([
                                "gpt-4",
                                "gpt-4-turbo-preview",
                                "gpt-4-0125-preview",
                                "gpt-3.5-turbo-0613",
                                "gpt-4-1106-preview",
                                "gpt-3.5-turbo-1106",
                                "gpt-3.5-turbo-16k",
                                "gpt-4-vision-preview",
                            ])
                            .default("gpt-4-turbo-preview"),
                        key: z.string().default(OPENAI_API_KEY),
                        guard: z.boolean().default(false),
                        n: z.number().default(1),
                    }),
                }),
                z.object({
                    type: z.literal("message"),
                    data: z.object({
                        role: z.enum(["user", "assistant", "system"]),
                        content: z.string(),
                    }),
                }),
                z.object({
                    type: z.literal("tool"),
                    force: z.boolean().default(false),
                    data: z.object({
                        type: z.literal("function"),
                        function: z.object({
                            name: z.string(),
                            function: z.string(),
                            parse: z.string(),
                            description: z.string(),
                            parameters: z.object().passthrough(),
                        }),
                    }),
                }),
                z
                    .object({
                        type: z.string().default("context"),
                    })
                    .passthrough()
                    .transform((o) => ({
                        type: "message",
                        data: {
                            role: "system",
                            content: [
                                `This is a context message. It may or may not be relevant to the conversation at hand. Use your discretion whether to consider it when responding to the user.`,
                                JSON.stringify(o, null, 2),
                            ].join("\n"),
                        },
                    })),
            ])
        )
    );

export const connect = (program) =>
    pipe(
        // tap((input) => console.log("GPT GUARD GOT INPUT", input.packets)),
        map((evaluation) => ({
            ...evaluation,
            packets: evaluation.packets.concat([
                {
                    type: "tool",
                    force: true,
                    data: {
                        type: "function",
                        function: {
                            name: "answer_question",
                            function: `(parameters, runner) => runner.abort()`,
                            parse: `JSON.parse(args)`,
                            description:
                                "invoke this function to answer the users question",
                            parameters: {
                                type: "object",
                                description:
                                    "your answer to the last question posed by the user.",
                                properties: {
                                    explanation: {
                                        type: "string",
                                    },
                                    bool: {
                                        type: "boolean",
                                        description:
                                            "if the answer is YES, set to true. If the answer is NO, set to false",
                                    },
                                },
                            },
                        },
                    },
                },
            ]),
        })),
        catchError((e) => {
            console.error(e);
        }),
        // tap(console.log.bind(console, program.id)),
        // tap(({ packets }) => console.log("GUARD INPUT", packets)),
        process(program),
        map((evaluation) => {
            const pass = evaluation.packets.some(({ data: { tool_calls } }) =>
                tool_calls?.some(
                    (call) =>
                        call.function.name === "answer_question" &&
                        JSON.parse(call.function.arguments).bool
                )
            );

            // console.log('GUARD RESULT', program.id, answer);

            return { pass, evaluation };
        })
    );

const guard = async (parameters, runner, evaluation) => {
    runner.abort();
    await evaluation.incrementalPatch({
        complete: true,
        terminal: !parameters.bool,
    });
};

const guardTool = {
    type: "tool",
    force: true,
    data: {
        type: "function",
        function: {
            name: "answer_question",
            function: guard.toString(),
            parse: `JSON.parse(args)`,
            description: "invoke this function to answer the users question",
            parameters: {
                type: "object",
                description:
                    "your answer to the last question posed by the user.",
                properties: {
                    explanation: {
                        type: "string",
                    },
                    bool: {
                        type: "boolean",
                        description:
                            "if the answer is YES, set to true. If the answer is NO, set to false",
                    },
                },
            },
        },
    },
};

export const process = (program) => {
    return (input$) => {
        const config$ = combineLatest(
            program.get$("config").pipe(filter(Boolean), startWith({})),
            schemas(program)
        ).pipe(
            map(([config, schema]) => {
                // console.log("parse config data", config);
                return schema.parse({ type: "config", data: config });
            }),
            map(({ data }) => data),
            filter(({ key, prompt }) => key !== OPENAI_API_KEY && prompt),
            distinctUntilChanged(deepEqual),
            // tap((c) => console.log("parsed config data", program.id, c)),
            shareReplay(1)
        );

        return concat(
            input$.pipe(
                buffer(config$),
                take(1),
                switchMap((inputs) => from(inputs))
                // tap(() => console.log("BUFFERED INPUT"))
            ),
            input$
        ).pipe(
            // tap(console.log.bind(console, program.id, "withLatestSchema")),
            mergeMap(async (trigger) => ({
                trigger,
                evaluation: await firstValueFrom(trigger.createEval()),
            })),
            mergeMap(async ({ trigger, evaluation }) => ({
                trigger,
                evaluation,
                context: await evaluation.getContext(),
            })),
            // tap(console.log.bind(console, program.id, "mapped input")),
            withLatestFrom(config$, schemas(program)),
            // tap(console.log.bind(console, program.id, "withLatest config")),
            mergeMap(([{ trigger, evaluation, context }, config, schemas]) => {
                // console.log("CONTEXT", context);
                const openai = new OpenAI({
                    apiKey: config.key,
                    dangerouslyAllowBrowser: true,
                });

                const messages = context
                    .map(({ packets }) => packets || [])
                    .flat()
                    .filter(({ type }) => type !== "tool")
                    .map(schemas.parse)
                    .map(({ data }) => data)
                    .concat({
                        role: config.role,
                        content: config.prompt,
                    });

                const _tools = context
                    .map(({ packets }) => packets || [])
                    .flat()
                    .filter(({ type }) => type === "tool");

                if (config.guard) {
                    _tools.push(guardTool);
                }
                // console.log("MESSAGES", messages);
                // console.log("TOOLS", _tools);
                const fKey = _tools.length > 0 ? "runTools" : "stream";

                const tools = _tools.map(
                    ({
                        data: {
                            type,
                            function: { function: fnStr, parse: pStr, ...def },
                        },
                    }) => ({
                        type,
                        function: {
                            function: async (parameters, runner) => {
                                // console.log(
                                //     "got tool invokation",
                                //     parameters,
                                //     runner
                                // );
                                const fn = new Function(
                                    "parameters",
                                    "runner",
                                    "evaluation",
                                    `return (${fnStr})(parameters,runner,evaluation)`
                                );

                                const res = await fn(
                                    parameters,
                                    runner,
                                    evaluation
                                );
                                // console.log("result", res);
                                return res;
                            },
                            parse: (args) => {
                                // console.log("got args", args);
                                const parse = new Function("args", pStr);
                                return JSON.parse(args);
                            },
                            ...def,
                        },
                    })
                );

                if (config.n > 1) {
                    return from(
                        openai.chat.completions.create({
                            messages,
                            model: config.model,
                            temperature: config.temperature,
                            n: config.n,
                        })
                    ).pipe(
                        switchMap((res) => {
                            const evals$ = concat(
                                of(evaluation),
                                trigger.createEvals(config.n - 1)
                            );

                            const pre = [messages.pop()];

                            const messages$ = from(
                                res.choices.map(({ message }) =>
                                    pre.concat([message])
                                )
                            );

                            return zip(messages$, evals$);
                        }),
                        take(config.n),
                        tap(([messages, evaluation]) =>
                            evaluation.incrementalPatch({
                                state: {
                                    messages: messages,
                                    complete: true,
                                },
                                packets: messages.map((msg) => ({
                                    type: "message",
                                    data: msg,
                                })),
                                complete: true,
                            })
                        )
                    );
                }

                const runOpts = {
                    stream: true,
                    messages,
                    model: config.model,
                    temperature: config.temperature,
                    ...(fKey === "runTools" ? { tools } : {}),
                    ...(fKey === "runTools" &&
                    tools.length === 1 &&
                    _tools[0].force
                        ? {
                              tool_choice: {
                                  type: "function",
                                  function: {
                                      name: tools[0].function.name,
                                  },
                              },
                          }
                        : {}),
                };

                // console.log(evaluation.node, runOpts);

                const runner = openai.beta.chat.completions[fKey](runOpts);

                const end$ = fromEvent(runner, "end").pipe(
                    tap(() => {
                        evaluation.incrementalPatch({
                            state: {
                                messages: runner.messages,
                            },
                            complete: true,
                        });
                        deltas.unsubscribe();
                    })
                );

                const deltas = fromEvent(
                    runner,
                    "content",
                    (delta, snapshot) => ({
                        delta,
                        snapshot,
                    })
                )
                    .pipe(
                        concatMap(async (data) => {
                            const latest = await evaluation.getLatest();
                            return latest.patch({
                                state: {
                                    ...evaluation.state,
                                    ...data,
                                },
                            });
                        }),
                        takeUntil(end$)
                    )
                    .subscribe();

                // console.log("send messages!?!?!??!", messages);
                return from(
                    runner
                        .finalMessage()
                        .then(() =>
                            runner.messages
                                .slice(messages.length - 1)
                                .map((data) => ({ type: "message", data }))
                        )
                        .then((packets) =>
                            evaluation.incrementalPatch({
                                packets,
                            })
                        )
                );
            }),
            catchError((e) => {
                console.log("error", e);
                return EMPTY;
            })
        );
    };
};

export default process;
