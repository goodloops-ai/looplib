import { Operable } from "./modules/operable.mjs";
import { of } from "rxjs";

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
(async () => {
    const filename = Deno.env.get("DENO_REPL_HISTORY");
    if (filename) {
        let lastText = await Deno.readTextFile(filename);
        idAllConsts(lastText, 1000);
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

const input$ = new Operable(of({ type: "input", data: { id: "input" } }));
