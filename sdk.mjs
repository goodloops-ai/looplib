import { db, process as flow, getIO, queryThread } from "./modules/flow.mjs";
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
} from "rxjs";
import { v4 as uuidv4 } from "uuid";
import { deepEqual } from "fast-equals";
import { take } from "npm:rxjs@^7.8.1";
import filenamify from "filenamify";
export class Workflow {
    constructor(id, env) {
        env.OPENAI_API_KEY = Deno.env.get("OPENAI_API_KEY");
        this.id = id;
        this.ready = Promise.all([
            db.nodes
                .upsert({
                    id,
                    process: "looplib/modules/flow.mjs",
                })
                .then((program) => {
                    this.program = program;
                    this.flowEvents$ = new Subject();
                    this.subscription = this.flowEvents$
                        .pipe(flow(this.program))
                        .subscribe();
                }),
            db.upsertLocal("ENV", env),
        ]);

        this.jobs = this.ready;
    }

    addNode(id, prompt, config = {}) {
        console.log("import.meta.url", import.meta.url);
        this.jobs = this.jobs.then(() => {
            return db.nodes.upsert({
                id,
                flow: this.program.id,
                process: "looplib/modules/gpt.mjs",
                config: {
                    ...config,
                    prompt,
                },
            });
        });

        return this;
    }

    connect(source, target, guard) {
        this.jobs = this.jobs.then(() => {
            return db.connections.upsert({
                id: uuidv4(),
                flow: this.program.id,
                source,
                target,
                connect: guard ? "looplib/modules/gpt.mjs" : undefined,
                config: guard
                    ? {
                          prompt: guard,
                      }
                    : undefined,
            });
        });

        return this;
    }

    async execute(prompt) {
        await this.jobs;

        const inputs = await firstValueFrom(
            getIO(this.program).pipe(
                switchMap(({ inputs }) => db.nodes.findByIds(inputs).exec())
            )
        );

        for (const [id, node] of inputs) {
            // console.log(node);
            await node.incrementalPatch({
                input: prompt
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
        }

        const $ = getIO(this.program).pipe(
            filter(({ outputs }) => {
                // console.log(
                // 	'got outputs',
                // 	outputs.map(id => id),
                // );
                return outputs.length;
            }),
            switchMap(({ outputs }) =>
                merge(
                    ...outputs.map(
                        (id) =>
                            db.states.find({
                                selector: {
                                    entity: id,
                                    packets: {
                                        $exists: true,
                                    },
                                },
                            }).$
                    )
                )
            ),
            filter((docs) => docs?.length),
            switchMap((docs) =>
                merge(...docs.map((doc) => doc.get$("packets")))
            )
        );

        return {
            $,
            value: await firstValueFrom($),
        };
    }

    output(path) {
        const sessionDate = new Date();

        this.jobs = this.jobs.then(() => {
            db.states
                .find({
                    selector: {
                        flow: this.program.id,
                        parent: {
                            $exists: false,
                        },
                    },
                })
                .$.pipe(switchMap((docs) => merge(...docs.map(queryThread))))
                .subscribe((thread) => {
                    const json = JSON.stringify(thread, null, 2);
                    const encoder = new TextEncoder();
                    Deno.writeFileSync(
                        `${path}-${filenamify(sessionDate.toString())}.json`,
                        encoder.encode(json)
                    );
                });
        });

        return this;
    }

    log() {
        this.jobs.then(() => {
            const incomplete$ = db.states.find({
                selector: {
                    flow: this.program.id,
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
                        merge(...docs.map((doc) => doc.get$("data"))).pipe(
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
