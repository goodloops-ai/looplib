import { Operable, Trigger } from "./modules/operable.mjs";
import { switchMap, take, debounceTime, Subject } from "rxjs";

try {
    const { load } = await import(
        "https://deno.land/std@0.214.0/dotenv/mod.ts"
    );
    await load({ export: true });
    console.log("loaded .env", window.Deno?.env?.get?.("OPENAI_API_KEY"));
} catch (e) {}

const { prompt, ask, guard } = await import("./modules/gpt.mjs");

function getAllConstVariableNames(code) {
    const regex = /const\s+([a-zA-Z_$][0-9a-zA-Z_$]*)\s*=/gm;
    let match;
    const matches = [];
    while ((match = regex.exec(code)) !== null) {
        matches.push(match[1]);
    }
    return matches;
}

async function idAllConsts(code) {
    const vars = getAllConstVariableNames(code);
    for (const v of vars) {
        let i = 0;
        while (i < 100) {
            await new Promise((resolve) => setTimeout(resolve, 0));
            try {
                eval(`${v}.id = "${v}"`);
                break;
            } catch (e) {
                i++;
            }
        }
    }
}

const input$ = new Operable(() => true);
input$.id = "input$";

(async () => {
    const filename = Deno.env.get("DENO_REPL_HISTORY");
    if (filename) {
        let lastText = await Deno.readTextFile(filename);
        await idAllConsts(lastText, 1000);
        await restoreState();
        for await (const event of Deno.watchFs(filename)) {
            if (event.kind === "modify") {
                // The file has been modified, read the file
                const text = await Deno.readTextFile(filename);
                const diff = text.slice(lastText.length);
                idAllConsts(diff);
                lastText = text;
            }
        }
    }
})();

async function restoreState() {
    const stateFile =
        Deno.env.get("DENO_REPL_STATE") ||
        Deno.env.get("DENO_REPL_HISTORY") + ".state";

    const state = await Deno.readTextFile(stateFile).catch((e) => null);
    const trigger = state
        ? Trigger.deserializeGraph(state)
        : new Trigger(0, input$);

    input$.next(trigger);

    trigger.toJson$().subscribe((json) => {
        Deno.writeTextFile(stateFile, json);
    });
}
