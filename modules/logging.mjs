import { tap } from "rxjs";

const DEBUG = Deno.env.get("DEBUG");

export const log = (doc, msg) => tap(() => DEBUG && console.log(doc.id, msg));
