import { Database, Q } from '@nozbe/watermelondb';
import { SyncDatabaseChangeSet, synchronize as watermelonSync } from '@nozbe/watermelondb/sync';
import { keys, map, omit } from 'lodash';
import { CollectionRef, FirestoreModule } from './types/firestore';
import { Item, SyncObj } from './types/interfaces';

/* const ex: SyncObj = {
    todos: {
        excludedFields: [],
        customQuery: firestore.collection('todos').where('color', '==', 'red'),
    },
} */

const defaultExcluded = ['_status', '_changed'];

export class SyncFireMelon {
    database: Database;
    syncObj: SyncObj;
    db: FirestoreModule;
    createSessionId: () => string;
    getTimestamp: () => any = () => new Date();
    watermelonSyncArgs: Object = {};

    constructor(
        database: Database,
        syncObj: SyncObj,
        db: FirestoreModule,
        createSessionId: () => string,
        getTimestamp: () => any = () => new Date(),
        watermelonSyncArgs: Object = {}
    ) {
        this.database = database;
        this.syncObj = syncObj;
        this.db = db;
        this.createSessionId = createSessionId;
        this.getTimestamp = getTimestamp;
        this.watermelonSyncArgs = watermelonSyncArgs;
    }

    get lastPulledAt(): Promise<number | null> {
        return new Promise((s, _) => {
            //@ts-ignore -- localStorage is not yet exported from WatermelonDB
            this.database.localStorage.get('__watermelon_last_pulled_at').then(val => s(val))
        })
    }

    async pullChanges({ lastPulledAt = undefined, sessionId = undefined, syncTimestamp = new Date() }:
        { lastPulledAt?: number | undefined | null, sessionId?: string | undefined, syncTimestamp?: Date }) {
        if (lastPulledAt === undefined) {
            lastPulledAt = await this.lastPulledAt;
        }
        if (sessionId === undefined) {
            sessionId = this.createSessionId();
        }

        const lastPulledAtTime = new Date(lastPulledAt || 0);
        let changes = {};

        const collections = keys(this.syncObj);

        const assetOperations: (() => Promise<void>)[] = [];

        await Promise.all(
            map(collections, async (collectionName) => {
                const collectionOptions = this.syncObj[collectionName];
                const assetOptions = collectionOptions.asset ? collectionOptions.asset : false;
                const query = (collectionOptions.customPullQuery && collectionOptions.customPullQuery(this.db, collectionName))
                    || this.db.collection(collectionName);

                const [createdSN, deletedSN, updatedSN] = await Promise.all([
                    query.where('server_created_at', '>=', lastPulledAtTime).where('server_created_at', '<=', syncTimestamp).get(),
                    query.where('server_deleted_at', '>=', lastPulledAtTime).where('server_deleted_at', '<=', syncTimestamp).get(),
                    query.where('server_updated_at', '>=', lastPulledAtTime).where('server_updated_at', '<=', syncTimestamp).get(),
                ]);

                /**
                 * Rules:
                 * 1. don't create something that will be deleted in the same session
                 * 2. don't update something if it created
                 * 
                 * PS. It may seem convincing to also not delete something that was also just created. However, consider this case:
                 * - device 1: create doc A and sync -> server_created_at will be slightly larger than lastPulledAt because pull occurs before push
                 * - device 2: delete doc A and sync changes
                 * - device 1: sync -> doc A will be recognized as created, because of the diff in lastPulledAt and server_created_at, if we now omit the deletion we will not have this important change!
                 */
                const created = createdSN.docs
                    .filter((t) => t.data().sessionId !== sessionId && !deletedSN.docs.find((doc) => doc.id === t.id))
                    .map((createdDoc) => {
                        const data = createdDoc.data();

                        const ommited = [...defaultExcluded, ...(collectionOptions.excludedFields || [])];
                        const createdItem = omit(data, ommited);

                        if(assetOptions) assetOperations.push(async () => assetOptions.pull.create(data) )

                        return createdItem;
                    });

                const updated = updatedSN.docs
                    .filter(
                        (t) => t.data().sessionId !== sessionId && !createdSN.docs.find((doc) => doc.id === t.id),
                    )
                    .map((updatedDoc) => {
                        const data = updatedDoc.data();

                        const ommited = [...defaultExcluded, ...(collectionOptions.excludedFields || [])];
                        const updatedItem = omit(data, ommited);

                        if(assetOptions) assetOperations.push(async () => assetOptions.pull.update(data) )

                        return updatedItem;
                    });

                const deleted = deletedSN.docs
                    .filter((t) => t.data().sessionId !== sessionId)
                    .map((deletedDoc) => {
                        const data = deletedDoc.data();
                        if(assetOptions) assetOperations.push(async () => assetOptions.pull.delete(data) )

                        return deletedDoc.id;
                    });

                changes = {
                    ...changes,
                    [collectionName]: { created, deleted, updated },
                };
            }),
        );

        // First execute the asset changes, if that completes successfully proceed with the Watermelon changes.
        console.log(`FireMelon > pull assets > Will commit a total of ${assetOperations.length} asset changes`);
        for(const assetOperation of assetOperations){
            await assetOperation();
        }

        const totalChanges = Object.keys(changes).reduce((prev, curr) =>
            //@ts-ignore
            prev + changes[curr].created.length + changes[curr].deleted.length + changes[curr].updated.length,
            0);
        console.log(`FireMelon > Pull > Total changes: ${totalChanges}`);

        return {changes, totalChanges}
    }

    // Private!!
    async _pushChanges({ changes, sessionId, lastPulledAt }:
        { changes: SyncDatabaseChangeSet, sessionId: string, lastPulledAt: number }) {

        const totalChanges = Object.keys(changes).reduce((prev, curr) =>
            prev + changes[curr].created.length + changes[curr].deleted.length + changes[curr].updated.length,
            0);
        console.log(`FireMelon > Push > Total changes: ${totalChanges}`);

        let docRefs = await Promise.all(Object.keys(changes).map(async (collectionName: string) => {
            const deletedIds = changes[collectionName].deleted.map(id => id);
            const createdIds = changes[collectionName].created.map(data => data.id);
            const updatedIds = changes[collectionName].updated.map(data => data.id);

            const collectionOptions = this.syncObj[collectionName];
            const collectionRef = (collectionOptions.customPushCollection && collectionOptions.customPushCollection(this.db, collectionName)) || this.db.collection(collectionName)

            const created = createdIds.length > 0 ? (await queryDocsInValue(collectionRef, 'id', createdIds)) : [];
            const deleted = deletedIds.length > 0 ? (await queryDocsInValue(collectionRef, 'id', deletedIds)) : [];
            const updated = updatedIds.length > 0 ? (await queryDocsInValue(collectionRef, 'id', updatedIds)) : [];

            return { [collectionName]: { created, deleted, updated } }
        }))

        // collapse to single object: {users: {deleted: [], updated: []}, todos: {deleted:[], updated:[]}}
        docRefs = Object.assign({}, ...docRefs);

        // Batch sync
        const batchArray: any[] = [];
        batchArray.push(this.db.batch());
        let operationCounter = 0;
        let batchIndex = 0;

        // 'Batch' assets
        const assetOperations: (() => Promise<void>)[] = [];

        map(changes, async (row, collectionName) => {
            // This iterates over all the collections, e.g. todos and users
            const collectionOptions = this.syncObj[collectionName];
            const assetOptions = collectionOptions.asset ? collectionOptions.asset : false;
            const collectionRef = (collectionOptions.customPushCollection && collectionOptions.customPushCollection(this.db, collectionName)) || this.db.collection(collectionName);

            map(row, async (arrayOfChanged, changeName) => {
                const isDelete = changeName === 'deleted';

                map(arrayOfChanged, async (wmObj) => {
                    const itemValue = isDelete ? null : (wmObj.valueOf() as Item);
                    const docId = isDelete ? wmObj.toString() : itemValue!.id;
                    const docRef = collectionRef.doc(docId);

                    const ommited = [
                        ...defaultExcluded,
                        ...(collectionOptions.excludedFields || []),
                    ];
                    const data = omit(itemValue, ommited);

                    switch (changeName) {
                        case 'created': {
                            //@ts-ignore
                            const docFromServer = docRefs[collectionName].created.find(doc => doc.id == data.id)
                            if (docFromServer) {
                                const warning = `${DOCUMENT_TRYING_TO_CREATE_ALREADY_EXISTS_ON_SERVER_ERROR} - document '${collectionName}' with id: '${data.id}'`
                                console.warn(warning);
                            }

                            batchArray[batchIndex].set(docRef, {
                                ...data,
                                server_created_at: this.getTimestamp(),
                                server_updated_at: this.getTimestamp(),
                                sessionId,
                            });

                            if(assetOptions) assetOperations.push(async () => assetOptions.push.create(data) )

                            operationCounter++;

                            break;
                        }

                        case 'updated': {
                            //@ts-ignore
                            const docFromServer = docRefs[collectionName].updated.find(doc => doc.id == data.id)
                            if (docFromServer) {
                                const { server_deleted_at: deletedAt, server_updated_at: updatedAt } = docFromServer;

                                if (updatedAt.toDate() > lastPulledAt) {
                                    const error = `${DOCUMENT_WAS_MODIFIED_ERROR} - document '${collectionName}' with id: '${data.id}' - updatedAt: ${updatedAt.toDate()}, lastPulledAt: ${lastPulledAt}`
                                    throw new Error(error);
                                }

                                if (deletedAt) throw new Error(DOCUMENT_WAS_DELETED_ERROR); // In line with 3a of Push Implementation (here)[https://nozbe.github.io/WatermelonDB/Advanced/Sync.html]
                                // if (deletedAt?.toDate() > lastPulledAt) {
                                //     throw new Error(DOCUMENT_WAS_DELETED_ERROR);
                                // }

                                batchArray[batchIndex].update(docRef, {
                                    ...data,
                                    sessionId,
                                    server_updated_at: this.getTimestamp(),
                                });

                                if(assetOptions) assetOperations.push(async () => assetOptions.push.update(data) )

                            } else {
                                const warning = `${DOCUMENT_TRYING_TO_UPDATE_BUT_DOESNT_EXIST_ON_SERVER_ERROR} - document '${collectionName}' with id: '${data.id}'`
                                console.warn(warning)

                                batchArray[batchIndex].set(docRef, {
                                    ...data,
                                    sessionId,
                                    server_updated_at: this.getTimestamp(),
                                });
                            }


                            operationCounter++;

                            break;
                        }

                        case 'deleted': {

                            //@ts-ignore
                            const docFromServer = docRefs[collectionName].deleted.find(doc => doc.id == wmObj.toString())
                            if (docFromServer) {
                                const { server_deleted_at: deletedAt, server_updated_at: updatedAt } = docFromServer;

                                if (updatedAt.toDate() > lastPulledAt) {
                                    throw new Error(DOCUMENT_WAS_MODIFIED_ERROR);
                                }

                                if (deletedAt?.toDate() > lastPulledAt) {
                                    throw new Error(DOCUMENT_WAS_DELETED_ERROR);
                                }

                                batchArray[batchIndex].update(docRef, {
                                    server_deleted_at: this.getTimestamp(),
                                    isDeleted: true,
                                    sessionId,
                                });

                                if(assetOptions) assetOperations.push(async () => assetOptions.push.delete(docFromServer) )

                            } else {
                                const warning = `${DOCUMENT_TRYING_TO_DELETE_BUT_DOESNT_EXIST_ON_SERVER_ERROR} - document '${collectionName}' with id: '${docId}'`
                                console.warn(warning)
                                // Will ignore in line with 4 of Push Implementation (here)[https://nozbe.github.io/WatermelonDB/Advanced/Sync.html]
                            }

                            operationCounter++;

                            break;
                        }
                    }

                    // Initialize a new batch if needed -> firestore allows 500 writes per batch.
                    if (operationCounter === 499) {
                        batchArray.push(this.db.batch());
                        batchIndex++;
                        operationCounter = 0;
                    }

                })
            })
        })

        // First execute the asset changes, if that completes successfully proceed with the Watermelon changes.
        console.log(`FireMelon > Push assets > Will commit a total of ${assetOperations.length} asset changes`);
        for(const assetOperation of assetOperations){
            await assetOperation()
        }

        console.log(`FireMelon > Push > Will commit ${batchArray.length} batches`)
        let counter = 1
        try {
            if (batchArray.length > 0) {
                for (const batch of batchArray) {
                    console.log(`FireMelon > Push > Batch ${counter} > commit`)
                    await batch.commit()
                    console.log(`FireMelon > Push > Commit batch ${counter} done`)
                    counter++;
                }
            } else {
                console.log('FireMelon > Push > Nothing to push')
            }
        } catch (error) {
            console.error(error);
        }
    }

    async synchronize(retry = true) {
        const sessionId = this.createSessionId();
        const sync = async () => await watermelonSync({
            database: this.database,
            ...this.watermelonSyncArgs,

            pullChanges: async ({ lastPulledAt }) => {
                const syncTimestamp = new Date();
                const { changes } = await this.pullChanges({ lastPulledAt, sessionId, syncTimestamp })

                return { changes, timestamp: +syncTimestamp };
            },

            pushChanges: async ({ changes, lastPulledAt }) => {
                await this._pushChanges({ changes, sessionId, lastPulledAt })
            },
        });

        try {
            await sync();
            return true
        } catch (error) {
            if (retry) {
                console.log("Firemelon > Sync error > retry sync");
                retry = false;
                await sync();
                return true
            } else {
                console.log("Firemelon > Sync failed");
                console.error(error);
                return false
            }
        }
    }
}

export const DOCUMENT_WAS_MODIFIED_ERROR = 'DOCUMENT WAS MODIFIED DURING PULL AND PUSH OPERATIONS';
export const DOCUMENT_WAS_DELETED_ERROR = 'DOCUMENT WAS DELETED DURING PULL AND PUSH OPERATIONS';
export const DOCUMENT_TRYING_TO_CREATE_ALREADY_EXISTS_ON_SERVER_ERROR = 'TYRING TO CREATE A DOCUMENT THAT ALREADY EXISTS ON THE SERVER - WILL OVERRIDE'
export const DOCUMENT_TRYING_TO_UPDATE_BUT_DOESNT_EXIST_ON_SERVER_ERROR = 'TYRING TO UPDATE A DOCUMENT BUT IT WAS NOT FOUND ON THE SERVER - WILL CREATE'
export const DOCUMENT_TRYING_TO_DELETE_BUT_DOESNT_EXIST_ON_SERVER_ERROR = 'TYRING TO DELETE A DOCUMENT BUT IT WAS NOT FOUND ON THE SERVER - IGNORE'

const queryDocsInValue = (collection: CollectionRef, field: string, array: any[]) => {
    return new Promise((res) => {
        // don't run if there aren't any ids or a path for the collection
        if (!array || !array.length || !collection || !field) return res([]);

        let batches = [];

        while (array.length) {
            // firestore limits batches to 10
            const batch = array.splice(0, 10);


            // add the batch request to to a queue
            batches.push(
                new Promise(response => {
                    collection
                        .where(
                            field,
                            //@ts-ignore
                            'in',
                            [...batch]
                        )
                        .get()
                        .then(results => {
                            response(results.docs.map(result => ({ ...result.data() })))
                        })
                        .catch((err) => {
                            console.error(err)
                        });
                })
            )
        }

        // after all of the data is fetched, return it
        Promise.all(batches)
            .then(content => res(content.flat()))
            .catch((err) => console.error(err));
    })
}