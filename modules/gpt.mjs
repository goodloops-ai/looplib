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
    timeout,
    pluck,
    Subject,
    ignoreElements,
    merge,
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
                            .default("gpt-4-0125-preview"),
                        key: z.string().default(OPENAI_API_KEY),
                        guard: z.boolean().default(false),
                        n: z.number().default(1),
                        includeAllContext: z.boolean().default(false),
                    }),
                }),
                z.object({
                    type: z.literal("message"),
                    data: z.object({
                        role: z.enum(["user", "assistant", "system", "tool"]),
                        content: z.union([z.string(), z.null()]).optional(),
                        tool_call_id: z.string().optional(),
                        tool_calls: z
                            .array(
                                z.object({
                                    id: z.string(),
                                    type: z.literal("function"),
                                    function: z.object({
                                        name: z.string(),
                                        arguments: z.string(),
                                    }),
                                })
                            )
                            .optional(),
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

const guard = async (parameters, runner, evaluation) => {
    runner.abort();
    await evaluation.incrementalPatch({
        complete: true,
        terminal: !parameters.answer,
    });
};

let total = 0;
let completed = 0;

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
                    answer: {
                        type: "boolean",
                        description:
                            "if the answer is YES, set to true. If the answer is NO, set to false",
                    },
                },
            },
        },
    },
};

let ends = 0;

const retryRunner = (fn, runOpts, evaluation, retries = 0) => {
    const runner = fn(runOpts);

    const timeout$ = new Subject();

    const end$ = fromEvent(runner, "end").pipe(
        map(() => runner),
        tap(() => console.log("END", ++ends)),
        timeout({
            each: 60 * 1000,
            with: () => {
                console.log("timeout", retries);
                timeout$.next(true);
                if (retries < 5) {
                    return retryRunner(fn, runOpts, evaluation, ++retries);
                } else {
                    return of(null);
                }
            },
        })
    );

    const deltas$ = fromEvent(runner, "content", (delta, snapshot) => ({
        delta,
        snapshot,
    })).pipe(
        // tap((data) => {
        //     console.log(data);
        // }),
        takeUntil(merge(end$, timeout$)),
        concatMap(async (data) => {
            return evaluation.incrementalPatch({
                state: {
                    ...evaluation.state,
                    ...data,
                },
            });
        }),
        ignoreElements()
    );

    return merge(end$, deltas$).pipe(
        take(1),
        tap((runner) => {
            if (runner.messages.length === runOpts.messages.length) {
                console.log("failed");
                evaluation.incrementalPatch({
                    packets: [
                        { type: "error", data: "failed to make gpt request" },
                    ],
                    complete: true,
                    terminal: true,
                });
            } else {
                console.log("complete");
                evaluation.incrementalPatch({
                    packets: runner.messages
                        .slice(runOpts.messages.length - 1)
                        .map((data) => ({ type: "message", data })),
                    complete: true,
                });
            }
        })
    );
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

        const client$ = config$.pipe(
            pluck("key"),
            distinctUntilChanged(),
            map(
                (key) =>
                    new OpenAI({
                        dangerouslyAllowBrowser: true,
                        apiKey: key,
                    })
            )
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
            withLatestFrom(config$, schemas(program), client$),
            // tap(console.log.bind(console, program.id, "withLatest config")),
            mergeMap(
                ([
                    { trigger, evaluation, context },
                    config,
                    schemas,
                    openai,
                ]) => {
                    // console.log("CONTEXT", context);

                    const messages = [
                        // {
                        //     role: "system",
                        //     content: [
                        //         `You must engage in a multithreaded conversation.`,
                        //         `Messages are organized into threads. System messages will direct your navigation through the conversation.`,
                        //         `Observe the "BEGIN_MESSAGE_GROUP" and "END_MESSAGE_GROUP" indicators to identify the commencement and termination of a message group.`,
                        //         `The "BEGIN_MESSAGE_GROUP" will contain a "GROUP_ID". Utilize this ID to correlate all messages within the group.`,
                        //         `The "END_MESSAGE_GROUP" denotes the end of a group and will restate the "GROUP_ID" for confirmation.`,
                        //         `All messages between these indicators constitute a single thread and must be treated as a unified conversation.`,
                        //         `System messages will enumerate "PARENT_GROUPS" to delineate the connections between message groups.`,
                        //         `Message groups are considered continuations of their associated "PARENT_GROUPS"`,
                        //         `Adhere strictly to these system messages to navigate the threaded conversation accurately, particularly when threads merge or are isolated.`,
                        //         `NEVER insert "END_MESSAGE_GROUP" messages yourself; this will be taken care of for you`,
                        //     ].join("\n"),
                        // },
                    ].concat(
                        context
                            .map(({ packets, id, parents }) =>
                                (packets
                                    ? [
                                          //   {
                                          //       type: "message",
                                          //       data: {
                                          //           role: "system",
                                          //           content: [
                                          //               `BEGIN_MESSAGE_GROUP`,
                                          //               `GROUP_ID: ${id}`,
                                          //               `PARENT_GROUPS:`,
                                          //               parents,
                                          //           ]
                                          //               .flat()
                                          //               .join("\n"),
                                          //       },
                                          //   },
                                      ]
                                    : []
                                )
                                    .concat(packets || [])
                                    .concat(
                                        packets
                                            ? [
                                                  //   {
                                                  //       type: "message",
                                                  //       data: {
                                                  //           role: "system",
                                                  //           content: [
                                                  //               `END_MESSAGE_GROUP`,
                                                  //               `GROUP_ID: ${id}`,
                                                  //           ].join("\n"),
                                                  //       },
                                                  //   },
                                              ]
                                            : []
                                    )
                            )
                            .flat()
                            .filter(({ type }) =>
                                config.includeAllContext
                                    ? type !== "tool"
                                    : type === "message"
                            )
                            .map(schemas.parse)
                            .map(({ data }) => data)
                            .concat([
                                // {
                                //     role: "system",
                                //     content: [
                                //         `BEGIN_MESSAGE_GROUP`,
                                //         `GROUP_ID: ${evaluation.id}`,
                                //         `PARENT_GROUPS:`,
                                //         evaluation.parents,
                                //     ]
                                //         .flat()
                                //         .join("\n"),
                                // },
                                {
                                    role: config.role,
                                    content: config.prompt,
                                },
                            ])
                    );

                    const _tools = context
                        .map(({ packets }) => packets || [])
                        .flat()
                        .filter(({ type }) => type === "tool");

                    if (config.guard) {
                        _tools.push(guardTool);
                    }
                    // console.log("dispatch GPT REQUEST", ++total);
                    // console.log("TOOLS", _tools);
                    const fKey = _tools.length > 0 ? "runTools" : "stream";

                    const tools = _tools.map(
                        ({
                            data: {
                                type,
                                function: {
                                    function: fnStr,
                                    parse: pStr,
                                    ...def
                                },
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

                    return retryRunner(
                        openai.beta.chat.completions[fKey].bind(
                            openai.beta.chat.completions
                        ),
                        runOpts,
                        evaluation
                    );
                },
                100
            ),
            catchError((e) => {
                console.log("error", e);
                return null;
            })
        );
    };
};

export default process;
