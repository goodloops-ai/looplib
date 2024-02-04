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
        n: 5,
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

const join = await db.nodes.upsert({
    id: "join",
    flow,
    join: "makeSubject",
    operator: "looplib/modules/gpt.mjs",
    parents: ["makeHaiku", "makeLimmerick"],
    config: {
        prompt: "please repeat all subjects, haikus, and limmericks",
    },
});

initNode({ node: makeHaiku, session });
initNode({ node: makeLimmerick, session });
initNode({ node: makeSubject, session });
initNode({ node: join, session });

let total = 0;
const haikusSub = db.evaluations
    .find({
        selector: {
            node: join.id,
            complete: true,
        },
    })
    .$.pipe(
        mergeMap((res) => from(res)),
        distinct(({ id }) => id)
    )
    .subscribe(async (haikuEvalDoc) =>
        console.log(
            "join",
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

await db.triggers.upsert({
    id: uuidv4(),
    node: makeSubject.id,
    flow,
    root: "root3",
    session,
    packets: [
        {
            type: "message",
            data: {
                role: "user",
                content: "I like literature",
            },
        },
    ],
});

db.upsertLocal("ENV", {
    OPENAI_API_KEY: Deno.env.get("OPENAI_API_KEY"),
});
