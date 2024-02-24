import { z } from "zod";
import { OpenAI } from "openai";
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
    concurrency: z.number().default(10),
    maxRetries: z.number().default(10),
    timeout: z.number().default(120 * 1000),
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

    return mergeMap(({ trigger, messages }) => {
        const openai = new OpenAI({
            dangerouslyAllowBrowser: true,
            apiKey: options.apiKey,
            maxRetries: options.maxRetries,
            timeout: options.timeout,
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
                        const response = runner.messages.slice(messages.length);
                        return { response };
                    });
                } else {
                    const response = runners
                        .map((runner) => runner.messages.slice(messages.length))
                        .flat();

                    return [{ response }];
                }
            })
        );
    }, options.concurrency);
};

const makeCall = (fn, runOpts, trigger) => {
    return mergeMap((idx) => {
        const runner = fn(runOpts);

        const end$ = fromEvent(runner, "end").pipe(map(() => runner));

        const error$ = fromEvent(runner, "error").pipe(
            tap((e) => console.error(e)),
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
    role: z.enum(["user", "assistant", "system"]).default("user"),
    context: z.enum(["complete", "partial"]).default("partial"),
});

export const promptGPT = (options) => {
    options = promptSchema.parse(options);
    const thisMsg = {
        role: options.role,
        content: options.prompt,
    };

    return pipe(
        map((trigger) => {
            // console.log("prompt");
            let messages =
                options.context === "complete"
                    ? trigger.findOne(completeContextSchema)?.messages || []
                    : trigger
                          .find(partialContextSchema)
                          .map(({ messages }) => messages)
                          .reverse()
                          .flat();

            messages = messages.concat([thisMsg]);

            return { trigger, messages };
        }),
        callGPT(options),
        map((responses) =>
            responses.map(({ response }) => ({
                type: "partial",
                messages: [thisMsg].concat(response),
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
