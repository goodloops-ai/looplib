async function startDenoRepl(evalFilePath, historyFilePath) {
    // Set the DENO_REPL_HISTORY environment variable
    Deno.env.set("DENO_REPL_HISTORY", historyFilePath);
    let historyCode = "";
    try {
        const history = await Deno.readTextFile(historyFilePath);
        // Concatenate the history commands into a single string
        historyCode = history
            .split("\n")
            .slice(1)
            .filter((line) => line.trim())
            .join(";");
    } catch (error) {
        console.error("Failed to read history file:", error);
    }

    // Prepare the subprocess arguments
    const args = ["repl", "-A", "--eval", historyCode];
    if (evalFilePath) {
        args.push("--eval-file=" + evalFilePath);
    }

    // Start the Deno REPL as a subprocess with inherited stdio
    const p = Deno.run({
        cmd: ["deno", ...args],
        stdin: "piped",
        stdout: "inherit",
        stderr: "inherit",
    });

    // Pipe the parent's stdin to the child's stdin
    await Deno.copy(Deno.stdin, p.stdin);

    // Wait for the subprocess to finish
    const status = await p.status();

    // Return the subprocess's exit status
    return status;
}

// Example usage:
// You can replace these with the actual file paths or arguments you receive
const evalFilePath = "./modules/repl.preamble.mjs"; //"path/to/your/eval/script.js";
const historyFilePath = "history.txt"; //"path/to/your/history/file.txt";

// Start the REPL
startDenoRepl(evalFilePath, historyFilePath).then((status) => {
    console.log(`REPL exited with code ${status.code}`);
});
