import { db, process as flow, initNode } from "./modules/flow.mjs";
import {
    firstValueFrom,
    merge,
    switchMap,
    tap,
    skip,
    filter,
    Subject,
    distinctUntilChanged,
    scan,
    map,
    mergeMap,
    from,
    distinct,
    take,
} from "rxjs";
import { v4 as uuidv4 } from "uuid";
import { deepEqual } from "fast-equals";
import filenamify from "filenamify";
try {
    const { load } = await import(
        "https://deno.land/std@0.214.0/dotenv/mod.ts"
    );
    await load({ export: true });
} catch (e) {}
const session = new Date().getTime();
export class Workflow {
    constructor(id, env = {}) {
        env.OPENAI_API_KEY =
            env.OPENAI_API_KEY ||
            window.Deno?.env?.get?.("OPENAI_API_KEY") ||
            localStorage.getItem("OPENAI_API_KEY") ||
            prompt("OPENAI KEY:");

        this.id = id;
        this.ready = Promise.all([
            // db.nodes
            //     .upsert({
            //         id,
            //         operator: "looplib/modules/flow.mjs",
            //     })
            //     .then((program) => {
            //         this.program = program;
            //         this.flowEvents$ = new Subject();
            //         this.subscription = this.flowEvents$
            //             .pipe(flow(this.program))
            //             .subscribe();
            //     }),
            db.upsertLocal("ENV", env),
        ]);

        this.jobs = this.ready;
    }

    addNode(id, prompt, config = {}) {
        // console.log("import.meta.url", import.meta.url);
        const data =
            typeof prompt === "function"
                ? {
                      id,
                      flow: this.id,
                      operator: prompt.toString(),
                      join: typeof config === "string" ? config : undefined,
                  }
                : {
                      id,
                      flow: this.id,
                      operator: "looplib/modules/gpt.mjs",
                      join: typeof config === "string" ? config : undefined,
                      config: {
                          ...config,
                          prompt,
                      },
                  };

        this.jobs = this.jobs
            .then(() => {
                return db.nodes.upsert(data);
            })
            .then(async (node) => {
                return await new Promise((r) =>
                    setTimeout(() => r(node), 1000)
                );
            })
            .then((node) => initNode({ node, session }));

        return this;
    }

    connect(source, target, guard) {
        this.jobs = this.jobs.then(async () => {
            await new Promise((r) => setTimeout(r, 1000));
            const targetNode = await db.nodes
                .findOne({
                    selector: {
                        id: target,
                    },
                })
                .exec();
            if (guard) {
                const guardNode = await db.nodes.upsert({
                    id: uuidv4(),
                    flow: this.id,
                    parents: [source],
                    operator: "looplib/modules/gpt.mjs",
                    config: {
                        prompt: guard,
                        guard: true,
                    },
                });

                await initNode({ node: guardNode, session });

                await targetNode.incrementalPatch({
                    parents: (targetNode.parents || []).concat([guardNode.id]),
                });
            } else {
                console.log("set parents", targetNode.id, source);
                await targetNode.incrementalPatch({
                    parents: (targetNode.parents || []).concat([source]),
                });
                console.log("set parent", targetNode.id);
            }
        });

        return this;
    }

    async execute(prompt) {
        await this.jobs.catch((e) => {
            console.error(e);
            // Deno.exit(1);
        });

        const input = await db.nodes
            .findOne({
                selector: {
                    flow: this.id,
                    parents: {
                        $exists: false,
                    },
                },
            })
            .exec();

        const allNodes = await db.nodes
            .find({
                selector: {
                    flow: this.id,
                },
            })
            .exec();

        const outputs = [];

        for (const n of allNodes) {
            const downstream = await db.nodes
                .findOne({
                    selector: {
                        flow: this.id,
                        parents: {
                            $in: [n.id],
                        },
                    },
                })
                .exec();

            if (!downstream) {
                outputs.push(n.id);
            }
        }
        await db.triggers.upsert({
            id: uuidv4(),
            node: input.id,
            flow: this.id,
            root: "root1",
            session,
            packets: prompt
                ? [
                      {
                          type: "message",
                          data: {
                              role: "user",
                              content: prompt,
                          },
                      },
                  ]
                : [],
        });

        // console.log(nod
        const $ = db.evaluations
            .find({
                selector: {
                    node: {
                        $in: outputs,
                    },
                    complete: true,
                },
            })
            .$.pipe(
                mergeMap((all) => from(all)),
                distinct(({ id }) => id),
                map((evaluation) => evaluation.toJSON())
            );

        return {
            $,
            value: await firstValueFrom($),
        };
    }

    output(path) {
        this.jobs = this.jobs.then(async () => {
            const outputs = [];
            const allNodes = await db.nodes
                .find({
                    selector: {
                        flow: this.id,
                    },
                })
                .exec();
            for (const n of allNodes) {
                const downstream = await db.nodes
                    .findOne({
                        selector: {
                            flow: this.id,
                            parents: {
                                $in: [n.id],
                            },
                        },
                    })
                    .exec();

                if (!downstream) {
                    outputs.push(n.id);
                }
            }

            db.evaluations
                .find({
                    selector: {
                        flow: this.id,
                        node: {
                            $in: outputs,
                        },
                        complete: true,
                    },
                })
                .$.pipe(
                    mergeMap((all) => from(all)),
                    distinct(({ id }) => id)
                )
                .subscribe((evaluation) => {
                    const sessionDate = new Date();
                    const thread = evaluation.toJSON();
                    const json = JSON.stringify(thread, null, 2);
                    const encoder = new TextEncoder();
                    Deno.writeFileSync(
                        `${path}-${evaluation.node}-${filenamify(
                            sessionDate.toString()
                        )}.json`,
                        encoder.encode(json)
                    );
                });
        });

        return this;
    }

    log() {
        this.jobs.then(() => {
            const incomplete$ = db.evaluations.find({
                selector: {
                    flow: this.id,
                    complete: false,
                },
            }).$;

            incomplete$
                .pipe(
                    switchMap((docs) => {
                        return merge(
                            ...docs.map((doc) =>
                                doc
                                    .get$("complete")
                                    .pipe(filter(Boolean), take(1))
                            )
                        );
                    })
                )
                .subscribe(() => {
                    console.log("\n");
                });

            incomplete$
                .pipe(
                    filter(Boolean),
                    distinctUntilChanged((a, b) =>
                        deepEqual(
                            a.map(({ id }) => id),
                            b.map(({ id }) => id)
                        )
                    ),
                    switchMap((docs) =>
                        merge(...docs.map((doc) => doc.get$("state"))).pipe(
                            skip(1)
                        )
                    ),
                    filter(Boolean),
                    scan((newline, { delta, complete }) => {
                        if (complete && !newline) {
                            return true;
                        } else {
                            Deno.writeAllSync(
                                Deno.stdout,
                                new TextEncoder().encode(delta)
                            );
                            return false;
                        }
                    })
                )
                .subscribe();
        });
        return this;
    }

    async destroy() {
        await Promise.all(this.jobs);
        this.subscription.unsubscribe();
    }
}
