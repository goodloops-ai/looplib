import { Workflow } from "./sdk.mjs";

const workflow = new Workflow("program-1", {
    OPENAI_API_KEY: Deno.env.get("OPENAI_API_KEY"),
});

workflow
    .addNode("makeHaiku", "write a haiku")
    .addNode(
        "rateHaiku",
        "Pretend to be a haiku judge and provide a rating from 1 to 10. You MUST provide a number. This is an exercise in testing branching code to detect your rating, dont over think it"
    )
    .addNode("improveHaiku", "please improve the haiku")
    .addNode(
        "outputSuccess",
        "please restate the most recent haiku and its rating."
    )
    .connect("makeHaiku", "rateHaiku")
    .connect("rateHaiku", "improveHaiku", "is the rating less than a 10?")
    .connect("improveHaiku", "rateHaiku")
    .connect("rateHaiku", "outputSuccess", "is the rating exactly 10?")
    .output(`./testsdk`)
    .log();

const { value, $ } = await workflow.execute("I like space");

console.log("workflow complete", value);
