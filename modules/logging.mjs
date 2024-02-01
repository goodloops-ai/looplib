import { tap } from "rxjs";

const _DEBUG = Deno.env.get("DEBUG");
const DEBUG = !!_DEBUG && _DEBUG !== "false" && _DEBUG !== "0";

export const log = (doc, msg) => tap(() => DEBUG && console.log(doc.id, msg));
