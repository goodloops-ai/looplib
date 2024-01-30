import { createRxDatabase, addRxPlugin } from "rxdb";
import { getRxStorageDexie } from "rxdb/plugins/storage-dexie";
import { getRxStorageMemory } from "rxdb/plugins/storage-memory";
import { RxDBLocalDocumentsPlugin } from "rxdb/plugins/local-documents";
import { RxDBDevModePlugin } from "rxdb/plugins/dev-mode";

addRxPlugin(RxDBDevModePlugin);
addRxPlugin(RxDBLocalDocumentsPlugin);

const evaluationSchema = {
    version: 0,
    type: "object",
    primaryKey: "id",
    properties: {
        id: {
            type: "string",
            maxLength: 255,
        },
        node: {
            type: "string",
            ref: "node",
            maxLength: 255,
        },
        flow: {
            type: "string",
            maxLength: 255,
        },
        parents: {
            type: "array",
            ref: "evaluation",
            items: {
                type: "string",
            },
        },
        state: {
            type: "object",
            additionalProperties: true,
        },
        parent: {
            type: "string",
        },
        complete: {
            type: "boolean",
            default: false,
        },
        packets: {
            type: "array",
            items: {
                type: "object",
                properties: {
                    type: {
                        type: "string",
                    },
                    data: {
                        type: "object",
                        additionalProperties: true,
                    },
                },
            },
        },
    },
    required: ["id", "flow", "entity"],
    indexes: ["flow", "entity"],
};

const nodeSchema = {
    version: 0,
    type: "object",
    primaryKey: "id",
    properties: {
        id: {
            type: "string",
            maxLength: 255,
        },
        flow: {
            type: "string",
        },
        process: {
            type: "string",
            maxLength: 255,
        },
        wrapper: {
            type: "string",
            maxLength: 255,
        },
        config: {
            type: "object",
            additionalProperties: true,
        },
        input: {
            type: "array",
            items: {
                type: "object",
                additionalProperties: true,
            },
        },
        // Add other properties as needed
    },
    required: ["id", "flow"],
};

const connectionsSchema = {
    version: 0,
    type: "object",
    primaryKey: "id",
    properties: {
        id: {
            type: "string",
            maxLength: 255,
        },
        flow: {
            type: "string",
            maxLength: 255,
        },
        source: {
            type: "string",
            maxLength: 255,
        },
        target: {
            type: "string",
            maxLength: 255,
        },
        connect: {
            type: "string",
        },
        config: {
            type: "object",
            additionalProperties: true,
        },
        // Add other properties as needed
    },
    required: ["id", "flow", "source", "target"],
    indexes: ["source", "target", "flow"],
};

const config = {
    dbName: "flowdb",
    collections: {
        nodes: {
            schema: nodesSchema,
        },
        connections: {
            schema: connectionsSchema,
        },
        evaluations: {
            schema: evaluationsSchema,
        },
    },
};

export const db = await initializeDatabase(config).catch(console.error);

async function initializeDatabase(config) {
    let db;
    if (typeof indexedDB !== "undefined") {
        db = await createRxDatabase({
            name: config.dbName,
            storage: getRxStorageDexie(),
            localDocuments: true,
        });
    } else {
        db = await createRxDatabase({
            name: config.dbName,
            storage: getRxStorageMemory(),
            localDocuments: true,
        });
    }

    await db.addCollections(config.collections);

    return db;
}
