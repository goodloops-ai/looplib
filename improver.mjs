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
        prompt: "please come up with a more specific subject matter (this is just a smoke test, don't over think it)",
        temperature: 0.7,
        n: 1,
    },
});

const makeHaiku = await db.nodes.upsert({
    id: "makeHaiku",
    flow,
    operator: "looplib/modules/gpt.mjs",
    parents: ["makeSubject"],
    config: {
        prompt: "please write me a haiku about the subject",
    },
});

const makeLimmerick = await db.nodes.upsert({
    id: "makeLimmerick",
    flow,
    operator: "looplib/modules/gpt.mjs",
    parents: ["makeSubject"],
    config: {
        prompt: "please write me a limmerick about the subject",
    },
});

const judge = await db.nodes.upsert({
    id: "judge",
    flow,
    operator: "looplib/modules/gpt.mjs",
    parents: ["makeHaiku", "makeLimmerick", "improve"],
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
        prompt: "is the latest rating a 10?",
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
        prompt: "please improve the poem",
    },
});

const join = await db.nodes.upsert({
    id: "join",
    flow,
    operator: "looplib/modules/gpt.mjs",
    parents: ["is10"],
    join: "makeSubject",
    config: {
        prompt: "what are the subjects, and what are their 10 rated poems?",
    },
});

initNode({ node: makeHaiku, session });
initNode({ node: makeSubject, session });
initNode({ node: judge, session });
initNode({ node: is10, session });
initNode({ node: isUnder10, session });
initNode({ node: improve, session });
initNode({ node: join, session });
initNode({ node: makeLimmerick, session });

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
    .subscribe(async (haikuEvalDoc) =>
        console.log(
            "GOT 10",
            haikuEvalDoc.id,
            ++total,
            JSON.stringify(await haikuEvalDoc.getContext(), null, 2)
        )
    );

await db.triggers.upsert({
    id: uuidv4(),
    node: makeSubject.id,
    flow,
    root: "root1",
    session,
    packets: [
        {
            type: "message",
            data: {
                role: "user",
                content: "I like space",
            },
        },
    ],
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
