import { map, pipe } from "rxjs";
import { Trigger, operableFrom } from "./operable.mjs";
console.log("start");
export function get(query, hidden = false) {
    return map(function (trigger) {
        // console.log("GET", trigger.get(query), trigger.payload);
        const res = trigger.get(query);
        if (!res) {
            return;
        }
        return { result: res, hidden };
    });
}

function find(query) {
    return map(function (trigger) {
        return trigger.find(query);
    });
}

function findOne(query) {
    return map(function (trigger) {
        return trigger.findOne(query);
    });
}

export function not(fn) {
    return pipe(
        fn,
        map((res) => !res)
    );
}

export function passThrough(trigger) {
    return trigger;
}

export function conditional(conditions) {
    const input$ = operableFrom(passThrough);

    Object.entries(conditions).forEach(([key, value]) => {
        input$[key] = operableFrom(value);
        input$.pipe(input$[key]);
    });

    return input$;
}

export function maxLoops(max, bail$) {
    return guard(lessThan(max), bail$);
}

function lessThan(count) {
    return function (trigger) {
        // console.log("lessThan", trigger.find(this), this, count);
        return trigger.find(this).length < count;
    };
}

function guard(condition, bail$) {
    return function (trigger) {
        condition = condition.bind(this);
        const res = condition(trigger);
        return res || bail$.next(trigger);
    };
}

export function retry(count, bail$, cond = () => true) {
    return function (trigger) {
        const isRetry = trigger.find(this).length < count;
        if (isRetry && cond(trigger)) {
            return { retry: true, hidden: true };
        }

        return bail$.next(trigger);
    };
}

export function retryTo(count, to$, cond = () => true) {
    return async function (trigger) {
        console.log("RETRY TO", count);
        const isRetry = trigger.find(this).length < count;
        const res = await cond(trigger);
        if (res && isRetry) {
            return to$.next(
                new Trigger({ retry: true, hidden: true }, this, trigger)
            );
        }

        return { hidden: true };
    };
}
