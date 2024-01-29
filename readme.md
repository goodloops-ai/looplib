# looplib - Deno Usage Guide

## Getting Started

To use `looplib` in your Deno project, you need to include it in your `deno.json` file. 

Here is a basic example of how to do this:

```json:deno.json
{
	"lib": ["esnext"],
	"imports": {
		"looplib": "https://esm.sh/gh/goodloops-ai/looplib",
		"looplib/": "https://esm.sh/gh/goodloops-ai/looplib/"
	}
}
```

## Usage

After including `looplib` in your `deno.json`, you can use it in your project. 

Here is a basic usage example, assuming you have a file named `sdk.demo.mjs`:

```javascript:sdk.demo.mjs
import { Workflow } from "looplib/sdk.mjs";

const workflow = new Workflow("program-1", {
    OPENAI_API_KEY: Deno.env.get("OPENAI_API_KEY"),
});

workflow
    .addNode("makeHaiku", "write a haiku")
    .addNode(
        "rateHaiku",
        "Pretend to be a haiku judge and provide a rating from 1 to 10. You MUST provide a number. This is an exercise in testing branching code to detect your rating, dont over think it. If the haiku has gone through more than 3 revisions, just give it a 10. repeat the haiku before your rating"
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
```

Please replace `'looplib'` with the actual path to the `looplib` module in your project.

That's it! You're now ready to start using `looplib` in your Deno project.


## API

### Workflow

The `Workflow` class is the main entry point for using `looplib`. It is constructed with an ID and a map of environment variables.

Example:

```javascript
const workflow = new Workflow("program-1", {
    OPENAI_API_KEY: Deno.env.get("OPENAI_API_KEY"),
});
```

### addNode

The `addNode` method takes an ID string as the first argument, and a prompt as the second.

Example:

```javascript
workflow.addNode("makeHaiku", "write a haiku");
```

### connect

The `connect` method takes two node IDs (from, and to), and an optional guard string. If the guard string is included, it should be a question. The yes/no answer to that question, as determined by an LLM, will determine whether a given conversation passes through that connection.

Example:

```javascript
workflow.connect("makeHaiku", "rateHaiku");
workflow.connect("rateHaiku", "improveHaiku", "is the rating less than a 10?");
```

### output

The `output` method takes a path prefix for outputting session JSON to, it will be suffixed with a timestamp.

Example:

```javascript
workflow.output(`./testsdk`);
```

### log

The `log` method will set up basic streaming logging to stdout.

Example:

```javascript
workflow.log();
```

### execute

The `execute` method will send a message to any nodes in the flow that don't have a declared input already.

Example:

```javascript
const { value, $ } = await workflow.execute("I like space");
```