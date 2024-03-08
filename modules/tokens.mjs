import { encodingForModel } from "https://esm.sh/js-tiktoken";
const encoder_cache = {};
// from https://github.com/codergautam/openai-gpt-token-counter/tree/main
export function getChatGPTEncoding(
    messages = [],
    model = "gpt-3.5-turbo" | "gpt-4" | "gpt-4-32k"
) {
    if (!Array.isArray(messages))
        throw new Error(
            "Please pass an array of messages in valid format to the chat function. Refer to the documentation for help ( https://www.npmjs.com/package/openai-gpt-token-counter )"
        );
    const isGpt3 = model.startsWith("gpt-3.5");

    let encoder = encoder_cache[model];
    if (!encoder) {
        encoder = encodingForModel(model, {
            "<|im_start|>": 100264,
            "<|im_end|>": 100265,
            "<|im_sep|>": 100266,
        });
        encoder_cache[model] = encoder;
    }

    const msgSep = isGpt3 ? "\n" : "";
    const roleSep = isGpt3 ? "\n" : "<|im_sep|>";

    const serialized = [
        messages
            .map(({ name, role, content }) => {
                return `<|im_start|>${
                    name || role
                }${roleSep}${content}<|im_end|>`;
            })
            .join(msgSep),
        "<|im_start|>assistant",
    ].join(msgSep);
    let encoded = encoder.encode(serialized, "all");
    let decodedstrs = [];
    for (let token of encoded) {
        var tokenDecoded = encoder.decode([token]);
        decodedstrs.push(tokenDecoded);
    }
    return encoded.length;
}
