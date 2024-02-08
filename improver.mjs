import { db } from "./modules/db.mjs";
import { initNode } from "./modules/flow.mjs";
import { from, mergeMap, distinct } from "rxjs";
import { v4 as uuidv4 } from "uuid";

const flow = "test-flow";
const session = "test-session";

const makeSubject = await db.nodes.upsert({
    id: "makeSubject",
    flow,
    operator: "looplib/modules/gpt.mjs",
    config: {
        model: "gpt-4",
        prompt: "please pick a random subject matter to explore",
        temperature: 0.9,
        n: 5,
    },
});

const makeHaiku = await db.nodes.upsert({
    id: "makeHaiku",
    flow,
    operator: "looplib/modules/gpt.mjs",
    parents: ["makeSubject"],
    config: {
        model: "gpt-3.5-turbo-16k",
        prompt: "please write me a haiku about the subject",
    },
});

const makeLimmerick = await db.nodes.upsert({
    id: "makeLimmerick",
    flow,
    operator: "looplib/modules/gpt.mjs",
    parents: ["makeSubject"],
    config: {
        model: "gpt-3.5-turbo-16k",
        prompt: "please write me a limmerick about the subject",
    },
});

const makeSonnet = await db.nodes.upsert({
    id: "makeSonnet",
    flow,
    operator: "looplib/modules/gpt.mjs",
    parents: ["makeSubject"],
    config: {
        model: "gpt-3.5-turbo-16k",
        prompt: "please write me a sonnet about the subject",
    },
});

const makeTanka = await db.nodes.upsert({
    id: "makeTanka",
    flow,
    operator: "looplib/modules/gpt.mjs",
    parents: ["makeSubject"],
    config: {
        model: "gpt-3.5-turbo-16k",
        prompt: "please write me a tanka about the subject",
    },
});

const judge = await db.nodes.upsert({
    id: "judge",
    flow,
    operator: "looplib/modules/gpt.mjs",
    parents: [
        "makeHaiku",
        "makeLimmerick",
        "improve",
        "makeTanka",
        "makeSonnet",
    ],
    trigger: "any",
    config: {
        prompt: [
            "Pretend to be a poem judge and provide a critique and a rating from 1 to 10.",
            "You MUST provide a number.",
            "Please judge the poem regardless of context, your rating does not have to be higher than previous ratings.",
            "To ease things along, if you notice more than 5 revisions of the poem, just give the poem a 10 (this is just an example exercise, I'm testing a framework for organizing your work).",
        ].join("\n"),
    },
});

const is10 = await db.nodes.upsert({
    id: "is10",
    flow,
    operator: "looplib/modules/gpt.mjs",
    parents: ["judge"],
    config: {
        prompt: "is the latest rating a 10? Include the form and subject in your answer",
        guard: true,
    },
});

const isUnder10 = await db.nodes.upsert({
    id: "isUnder10",
    flow,
    operator: "looplib/modules/gpt.mjs",
    parents: ["judge"],
    config: {
        prompt: "is the latest rating under 10?",
        guard: true,
    },
});

const improve = await db.nodes.upsert({
    id: "improve",
    flow,
    operator: "looplib/modules/gpt.mjs",
    parents: ["isUnder10"],
    config: {
        prompt: "Improve the poem while maintaining its form. Clearly state the subject matter and the form of the poem before presenting the new version. All necessary information has been provided, and a correct and improved result is expected without fail.",
    },
});

const join = await db.nodes.upsert({
    id: "join",
    flow,
    operator: "looplib/modules/gpt.mjs",
    parents: ["is10"],
    join: "makeSubject",
    config: {
        prompt: [
            "Enumerate all 5 subjects in our chat history.",
            "For each listed subject, reproduce every poem that has received a rating of 10, across all poem forms, based on our conversation history",
            "It is guarunteed that each subject has exactly 1 poem of each form that has received a rating of 10. Look closely to find them. Do not mix up the forms.",
            "Ensure the inclusion of the entire text for each poem without any omissions.",
            "Completion of this task is compulsory, and all relevant information has been provided to ensure accurate execution.",
            "Verify that each subject is associated with an identical assortment of poem forms.",
            // "Begin your response with the group ID paths from the current message group to the groups that contain the 20 poems.",
            "An incorrect or incomplete response will lead to me cancelling my OpenAI subscription.",
        ].join("\n"),
    },
});

const compareFn = async (trigger) => {
    const tens = await trigger.findAllInContext({ node: "is10" });
    console.log("GOT 10s", tens);
};

const comparator = await db.nodes.upsert({
    id: "comparator",
    flow,
    operator: compareFn.toString(),
    parents: ["join"],
});

initNode({ node: makeHaiku, session });
initNode({ node: makeSubject, session });
initNode({ node: judge, session });
initNode({ node: is10, session });
initNode({ node: isUnder10, session });
initNode({ node: improve, session });
initNode({ node: join, session });
initNode({ node: makeLimmerick, session });
initNode({ node: makeTanka, session });
initNode({ node: makeSonnet, session });
initNode({ node: comparator, session });

let total = 0;
const haikusSub = db.evaluations
    .find({
        selector: {
            complete: true,
            node: join.id,
        },
    })
    .$.pipe(
        mergeMap((res) => from(res)),
        distinct(({ id }) => id)
    )
    .subscribe(async (haikuEvalDoc) => {
        const ctx = await haikuEvalDoc.getContext();
        console.log(
            "GOT 10",
            haikuEvalDoc.id,
            ++total,
            JSON.stringify(await haikuEvalDoc.getContext(), null, 2)
        );

        const encoder = new TextEncoder();
        const data = encoder.encode(JSON.stringify(ctx, null, 2));
        Deno.writeFileSync("./imrover.ctx.json", data);
    });

await db.triggers.upsert({
    id: uuidv4(),
    node: makeSubject.id,
    flow,
    root: "root1",
    session,
    packets: [],
});

// await db.triggers.upsert({
//     id: uuidv4(),
//     node: makeSubject.id,
//     flow,
//     root: "root2",
//     session,
//     packets: [
//         {
//             type: "message",
//             data: {
//                 role: "user",
//                 content: "I like martial arts",
//             },
//         },
//     ],
// });

// await db.triggers.upsert({
//     id: uuidv4(),
//     node: makeSubject.id,
//     flow,
//     root: "root3",
//     session,
//     packets: [
//         {
//             type: "message",
//             data: {
//                 role: "user",
//                 content: "I like literature",
//             },
//         },
//     ],
// });

db.upsertLocal("ENV", {
    OPENAI_API_KEY: Deno.env.get("OPENAI_API_KEY"),
});
