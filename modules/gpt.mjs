import { z } from "zod";
import { OpenAI } from "https://esm.sh/gh/goodloops-ai/openai-node@c35c92f32328586bc4c46adeaaee8b34aa2573b6";
import { getChatGPTEncoding } from "./tokens.mjs";
import {
    map,
    fromEvent,
    range,
    take,
    takeUntil,
    mergeMap,
    ignoreElements,
    ReplaySubject,
    merge,
    pipe,
    toArray,
    tap,
} from "rxjs";
import { addToBehaviorSubject, operableFrom } from "./operable.mjs";
import YAML from "yaml";

try {
    const { load } = await import(
        "https://deno.land/std@0.214.0/dotenv/mod.ts"
    );
    await load({ export: true });
    console.log("loaded .env", window.Deno?.env?.get?.("OPENAI_API_KEY"));
} catch (e) {}

export const OPENAI_API_KEY = "OPENAI_API_KEY";
export const ENV = [OPENAI_API_KEY];

const guardFn = async (parameters, runner, evaluation) => {
    runner.abort();
    return parameters;
};

const guardTool = {
    force: true,
    fn: {
        name: "answer_question",
        function: guardFn,
        description: "invoke this function to answer the users question",
        parameters: {
            type: "object",
            description: "your answer to the last question posed by the user.",
            properties: {
                explanation: {
                    description: "your explanation for the answer",
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
};

const toolsSchema = z.object({
    force: z.boolean(),
    fn: z
        .object({
            name: z.string(),
            description: z.string(),
            function: z.any(),
            parse: z.any().default(() => JSON.parse),
            parameters: z.any(),
        })
        .passthrough(),
});

// console.log("PREPARSE", guardTool);
// console.log("PARSE", toolsSchema.parse(guardTool));

const messagesSchema = z
    .array(
        z.object({
            role: z
                .enum(["user", "assistant", "system", "tool"])
                .default("user"),
            content: z.string().optional(),
            tool_calls: z.any(),
        })
    )
    .default([]);

export const completeContextSchema = z.object({
    messages: messagesSchema,
    type: z.literal("complete"),
});

export const partialContextSchema = z.object({
    messages: messagesSchema,
    type: z.literal("partial"),
});

const callSchema = z.object({
    apiKey: z.string().default(Deno.env.get("OPENAI_API_KEY")),
    model: z.string().default("gpt-4-turbo-preview"),
    temperature: z.number().default(0.3),
    n: z.number().default(1),
    branch: z.boolean().default(false),
    concurrency: z.number().default(100),
    maxRetries: z.number().default(10),
    timeout: z.number().default(120 * 1000),
    max_tokens: z.number().default(4000),
    tools: z
        .array(
            z.object({
                force: z.boolean(),
                fn: z
                    .object({
                        name: z.string(),
                        description: z.string(),
                        function: z.any(),
                        parse: z.any().default(() => JSON.parse),
                        parameters: z.any(),
                    })
                    .passthrough(),
            })
        )
        .default([]),
});

export const callGPT = (options) => {
    options = callSchema.parse(options);
    const useTogether = !options.model.includes("gpt");
    const tokenModel = useTogether ? "gpt-4" : options.model;

    const tokens = {
        model: tokenModel,
        request: 0,
        response: 0,
    };
    return mergeMap(
        ({ trigger, messages }) => {
            tokens.request = getChatGPTEncoding(messages, tokenModel);
            const requestIndex = messages.length;

            const openai = new OpenAI({
                dangerouslyAllowBrowser: true,
                apiKey: options.apiKey,
                maxRetries: options.maxRetries,
                timeout: options.timeout,
                ...(useTogether
                    ? {
                          baseURL: "https://api.together.xyz/v1",
                          apiKey: Deno.env.get("TOGETHER_API_KEY"),
                      }
                    : {}),
            });

            const tools = options.tools.map(({ fn }) => ({
                type: "function",
                function: fn,
            }));

            const runOpts = {
                stream: true,
                messages,
                model: options.model,
                temperature: options.temperature,
                max_tokens: options.max_tokens,
                ...(tools.length ? { tools } : {}),
                ...(tools.length === 1 && tools[0].force
                    ? {
                          tool_choice: {
                              type: "function",
                              function: {
                                  name: tools[0].fn.name,
                              },
                          },
                      }
                    : {}),
            };

            const runFn = tools.length
                ? openai.beta.chat.completions.runTools.bind(
                      openai.beta.chat.completions
                  )
                : openai.beta.chat.completions.stream.bind(
                      openai.beta.chat.completions
                  );

            // console.log("RUNOPTS", runOpts);
            return range(1, options.n).pipe(
                makeCall(runFn, runOpts, trigger),
                take(options.n),
                toArray(),
                map((runners) => {
                    if (options.branch) {
                        return runners.map((runner, idx) => {
                            const response = runner.messages.slice(
                                messages.length
                            );
                            const responseTokens = getChatGPTEncoding(
                                response,
                                tokenModel
                            );

                            return {
                                response,
                                tokens: {
                                    ...tokens,
                                    response: responseTokens,
                                },
                            };
                        });
                    } else {
                        const response = runners
                            .map((runner) =>
                                runner.messages.slice(messages.length)
                            )
                            .flat();

                        tokens.response = getChatGPTEncoding(
                            response,
                            tokenModel
                        );

                        return [{ response, tokens }];
                    }
                })
            );
        },
        useTogether ? 1 : options.concurrency
    );
};

const makeCall = (fn, runOpts, trigger) => {
    return mergeMap((idx) => {
        const runner = fn(runOpts);

        const end$ = fromEvent(runner, "end").pipe(map(() => runner));

        const error$ = fromEvent(runner, "error").pipe(
            tap((e) => console.error("GPT ERROR", e)),
            ignoreElements()
        );
        const abort$ = fromEvent(runner, "abort").pipe(ignoreElements());

        if (idx === 1) {
            const state$ = new ReplaySubject(1);
            addToBehaviorSubject(trigger.states$, state$);
            state$.next({ messages: runOpts.messages });

            fromEvent(runner, "content", (delta, snapshot) => ({
                delta,
                snapshot,
            }))
                .pipe(takeUntil(end$))
                .subscribe(state$);
        }

        return merge(end$, error$, abort$);
    });
};

const promptSchema = callSchema.extend({
    prompt: z.string(),
    system: z.string().optional(),
    role: z.enum(["user", "assistant", "system"]).default("user"),
    context: z.enum(["complete", "partial"]).default("complete"),
    reducer: z.boolean().default(false),
});

export const promptGPT = (options) => {
    options = promptSchema.parse(options);
    const thisMsg = {
        role: options.role,
        content: options.prompt,
    };

    let systemMessage = options.system
        ? { role: "system", content: options.system }
        : null;

    return pipe(
        map((trigger) => {
            // console.log("prompt");
            let blind;
            let messages =
                options.context === "complete"
                    ? trigger
                          .find()
                          .filter((data) => {
                              console.log("DATA", data, data.type, blind);
                              if (blind) return false;

                              if (data.type === "blind") {
                                  blind = true;
                                  return true;
                              }

                              return true;
                          })
                          .map((data) => {
                              if (data.messages) return data.messages;
                              if (!data || data === true || data?.hidden)
                                  return [];

                              console.log("YAML", data.toString(), data);
                              return {
                                  role: "user",
                                  content: YAML.stringify(data, null, 2),
                              };
                          })
                          .reverse()
                          .flat()
                    : trigger
                          .find(partialContextSchema)
                          .map(({ messages }) => messages)
                          .reverse()
                          .flat();

            if (options.reducer) {
                if (trigger.previous) {
                    const previousMessage =
                        trigger.previous[0].messages[
                            trigger.previous[0].messages?.length - 1
                        ];

                    const summary = {
                        role: "user",
                        content: `Here is your previous response:

\`\`\`markdown
${previousMessage.content}
\`\`\`

Augment this response.`,
                    };

                    messages = messages.concat([summary]);
                }
            }

            messages = messages.concat([thisMsg]);

            if (systemMessage) {
                messages = [systemMessage].concat(messages);
            }

            console.log("PROMPT", JSON.stringify(messages, null, 2));

            return { trigger, messages };
        }),
        callGPT(options),
        map((responses) =>
            responses.map(({ response, tokens }) => ({
                type: "partial",
                messages: [thisMsg].concat(response),
                tokens,
            }))
        )
    );
};

export function prompt(optionsOrPrompt) {
    const options =
        typeof optionsOrPrompt === "string"
            ? { prompt: optionsOrPrompt }
            : optionsOrPrompt;

    return operableFrom(promptGPT(options));
}

export function ask(question) {
    const tools = [guardTool];

    return prompt({ prompt: question, role: "user", tools });
}

export function guard(askNode, yes = true) {
    // console.log("GUARD", askNode.id, yes);
    return pipe(
        map((trigger) => {
            // console.log("GUARD", askNode.id, trigger.operable.id, yes);
            const context = trigger.payload;
            // console.log("CONTEXT", context);
            const { answer } = JSON.parse(
                context.messages.find(({ role }) => role === "tool").content
            );
            // console.log("GUARD", askNode.id, trigger.operable.id, yes, answer);

            return yes ? answer : !answer;
        })
    );
}
