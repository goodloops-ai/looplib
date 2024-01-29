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
export class Workflow {
    constructor(id, env) {
        env.OPENAI_API_KEY = Deno.env.get("OPENAI_API_KEY");
        this.id = id;
        this.ready = Promise.all([
            db.nodes
                .upsert({
                    id,
                    process: "./flow.mjs",
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
        this.jobs = this.jobs.then(() => {
            return db.nodes.upsert({
                id,
                flow: this.program.id,
                process: "./gpt.mjs",
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
                connect: guard ? "./gpt.mjs" : undefined,
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
                input: [
                    {
                        type: "message",
                        data: {
                            role: "user",
                            content: prompt,
                        },
                    },
                ],
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
                        `${path}-${sessionDate}.json`,
                        encoder.encode(json)
                    );
                });
        });

        return this;
    }

    log() {
        this.jobs.then(() => {
            db.states
                .find({
                    selector: {
                        flow: this.program.id,
                    },
                })
                .$.pipe(
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
                            console.log("\n\n");
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
