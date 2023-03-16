import { Database } from '@nozbe/watermelondb';
import { SyncDatabaseChangeSet, synchronize as watermelonSync } from '@nozbe/watermelondb/sync';
import { keys, map, omit } from 'lodash';
import { CollectionRef, Firestore, FirestoreModule } from './types/firestore';
import { Item, SyncObj } from './types/interfaces';
import { checkIdsExistence } from './utils/helpers';

const defaultExcluded = ['_status', '_changed'];

export class SyncFireMelon {
    database: Database;
    syncObj: SyncObj;
    firestore: Firestore;
    db: FirestoreModule;
    createSessionId: () => string;
    getTimestamp: () => any = () => new Date();
    watermelonSyncArgs: Object = {};

    constructor(
        database: Database,
        syncObj: SyncObj,
        firestore: Firestore,
        createSessionId: () => string,
        getTimestamp: () => any = () => new Date(),
        watermelonSyncArgs: Object = {}
    ) {
        this.database = database;
        this.syncObj = syncObj;
        this.db = firestore();
        this.firestore = firestore;
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

        console.log("FireMelon > Start pull");

        if (lastPulledAt === undefined) {
            lastPulledAt = await this.lastPulledAt;
        }
        if (sessionId === undefined) {
            sessionId = this.createSessionId();
        }

        const lastPulledAtTime = new Date(lastPulledAt || 0);
        console.log(`FireMelon > last pulled at: ${lastPulledAt}`);
        let changes = {};

        const collections = keys(this.syncObj);

        const assetOperations: (() => Promise<void>)[] = [];

        await Promise.all(
            map(collections, async (collectionName) => {
                const collectionOptions = this.syncObj[collectionName];
                const assetOptions = collectionOptions.asset ? collectionOptions.asset : false;
                const query = (collectionOptions.customPullQuery && collectionOptions.customPullQuery(this.db, collectionName))
                    || this.db.collection(collectionName);

                console.log(`FireMelon > WILL START PULL FOR ${collectionName}`);

                /**
                 * Make sure to create the relevant indices
                 */
                const [createdSN, updatedSN, deletedSN] = lastPulledAt == null
                    ? await Promise.all([
                        query.where('server_created_at', '<=', syncTimestamp).where('isDeleted', '==', false).get(),
                        { docs: [] },
                        { docs: [] }
                    ])
                    : await Promise.all([
                        // CREATED - 
                        query.where('server_created_at', '>=', lastPulledAtTime)
                            .where('server_created_at', '<=', syncTimestamp)
                            .where('isDeleted', '==', false).get(),
                        // UPDATED 
                        query.where('server_updated_at', '>=', lastPulledAtTime)
                            .where('server_updated_at', '<=', syncTimestamp)
                            .where('isDeleted', '==', false).get(),
                        // DELETED
                        query.where('server_deleted_at', '>=', lastPulledAtTime)
                            .where('server_deleted_at', '<=', syncTimestamp).get(),
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
                const created = createdSN.docs.reduce((prev, curr) => {
                    const data = curr.data();
                    if (data.sessionId !== sessionId) {
                        const ommited = [...defaultExcluded, ...(collectionOptions.excludedFields || [])];
                        const createdItem = omit(data, ommited);

                        if (assetOptions && assetOptions.pull?.create) {
                            //@ts-ignore
                            assetOperations.push(async () => assetOptions.pull.create(data))
                        }

                        prev.push(createdItem);
                    }

                    return prev
                }, [] as SyncObj[])

                const updated = updatedSN.docs.reduce((prev, curr) => {
                    const data = curr.data();
                    /**
                     * @todo - this should filter out for everything that is in created. But it doesn't seem to do so with the current error.
                     */
                    if (data.sessionId !== sessionId
                        && !(created.find(val => val.id === data.id))
                        // && data.server_created_at < lastPulledAtTime
                    ) {
                        const ommited = [...defaultExcluded, ...(collectionOptions.excludedFields || [])];
                        const updatedItem = omit(data, ommited);

                        if (assetOptions && assetOptions.pull?.update) {
                            //@ts-ignore
                            assetOperations.push(async () => assetOptions.pull.update(data))
                        }

                        prev.push(updatedItem);
                    }

                    return prev
                }, [] as SyncObj[])

                const deleted = deletedSN.docs.reduce((prev, curr) => {
                    const data = curr.data();
                    if (data.sessionId !== sessionId) {
                        if (assetOptions && assetOptions.pull?.delete) {
                            //@ts-ignore
                            assetOperations.push(async () => assetOptions.pull.delete(data))
                        }

                        prev.push(data.id);
                    }

                    return prev
                }, [] as SyncObj[])

                console.log(`FireMelon > Pull > ${collectionName} > created=${created.length}, deleted=${deleted.length}, updated=${updated.length}`)
                changes = {
                    ...changes,
                    [collectionName]: { created, deleted, updated },
                };
            }),
        );

        // First execute the asset changes, if that completes successfully proceed with the Watermelon changes.
        console.log(`FireMelon > Pull assets > Will commit a total of ${assetOperations.length} asset changes`);
        for (const assetOperation of assetOperations) {
            await assetOperation();
        }

        const totalChanges = Object.keys(changes).reduce((prev, curr) =>
            //@ts-ignore
            prev + changes[curr].created.length + changes[curr].deleted.length + changes[curr].updated.length,
            0);
        console.log(`FireMelon > Pull > Total changes: ${totalChanges}`);

        return { changes, totalChanges }
    }

    getCollectionRef(collectionName: string) {
        const collectionOptions = this.syncObj[collectionName];
        return (collectionOptions.customPushCollection && collectionOptions.customPushCollection(this.db, collectionName)) || this.db.collection(collectionName);
    }

    // Private!!
    async _pushChanges({ changes, sessionId, lastPulledAt }:
        { changes: SyncDatabaseChangeSet, sessionId: string, lastPulledAt: number }) {

        console.log(`FireMelon > Start push`);

        const totalChanges = Object.keys(changes).reduce((prev, curr) =>
            prev + changes[curr].created.length + changes[curr].deleted.length + changes[curr].updated.length,
            0);
        console.log(`FireMelon > Push > Total changes: ${totalChanges}`);

        // get the createItems that already exist
        // this is procedure introduced to account for created items during migration that can be introduced on all devices
        const existingCreateIdsEntries = await Promise.all(
            Object.entries(changes).map(async ([collectionName, changeSet]) => {
                const idsToVerify = changeSet.created.map(obj => (obj.valueOf() as Item).id);
                const ids = await checkIdsExistence(this.firestore, this.getCollectionRef(collectionName), idsToVerify)
                return [collectionName, ids];
            })
        )
        const existingCreateIds = Object.fromEntries(existingCreateIdsEntries);
        console.log("existingCreateIds")
        console.log(existingCreateIds)

        // Batch sync
        const batchArray: any[] = [];
        batchArray.push(this.db.batch());
        let operationCounter = 0;
        let batchIndex = 0;
        const maxPerBatch = 500; // This is a firebase limit

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
                            if (existingCreateIds[collectionName].includes(docId)) {
                                console.log(`Firemelon > create > ${collectionName} > id '${docId}' already exists > skip create`)
                                return;
                            }

                            batchArray[batchIndex].set(docRef, {
                                ...data,
                                server_created_at: this.getTimestamp(),
                                // server_updated_at: this.getTimestamp(), // delete this so that we do not get updated items when these are merely created
                                isDeleted: false,
                                sessionId,
                            })

                            if (assetOptions && assetOptions.push?.create) {
                                //@ts-ignore
                                assetOperations.push(async () => assetOptions.push.create(data))
                            }

                            operationCounter++;

                            break;
                        }

                        case 'updated': {

                            if (assetOptions && assetOptions.push?.update) {
                                //@ts-ignore
                                assetOperations.push(async () => assetOptions.push.update(data))
                            }

                            /**
                             * @note We do not throw any error if during pull and this subsequent push some data has changed. We will simply merge in the data. 
                             * This means that data that was pushed by another device exactly during the pull/push of this device may be erased. Yes, this is 
                             * a bug, and should be resolved.
                             * 
                             * The difficulty here is being able to run as a batch and still get information about the 'live' value the last update date on the server.
                             * Preferably we'd be able to do such check on the server. 
                             * 
                             */

                            /**
                             * we merge here, to make sure that if the document was deleted, that information remains alive on the server. 
                             * Doing so best matches point 3a of Push Implementation (here)[https://nozbe.github.io/WatermelonDB/Advanced/Sync.html]
                             *  */
                            batchArray[batchIndex].set(docRef, {
                                ...data,
                                sessionId,
                                // isDeleted: false,
                                server_updated_at: this.getTimestamp(),
                            }, { merge: true });

                            operationCounter++;

                            break;
                        }

                        case 'deleted': {

                            /**
                             * @note The same note as on the updated case holds
                             * 
                             * Also, if the document does not exist on the server, we may simply ignore creating it. However, doing so requires us to check existence on the server, 
                             * and that requires getting al data from the server upfront, which on its turn is expensive. Better to simply allow the creation of this deleted item 
                             * on the server and with a certain frequency clear the firestore from deleted items. This is something you will have to do anyways!
                             */
                            batchArray[batchIndex].set(docRef, {
                                ...data,
                                server_deleted_at: this.getTimestamp(),
                                isDeleted: true,
                                sessionId,
                            }, { merge: true });

                            if (assetOptions && assetOptions.push?.delete) {
                                //@ts-ignore
                                assetOperations.push(async () => assetOptions.push.delete(docFromServer))
                            }

                            operationCounter++;

                            break;
                        }
                    }

                    // Initialize a new batch if needed -> firestore allows 500 writes per batch.
                    if (operationCounter === (maxPerBatch - 1)) {
                        batchArray.push(this.db.batch());
                        batchIndex++;
                        operationCounter = 0;
                    }

                })
            })
        })

        // First execute the asset changes, if that completes successfully proceed with the Watermelon changes.
        console.log(`FireMelon > Push assets > Will commit a total of ${assetOperations.length} asset changes`);
        for (const assetOperation of assetOperations) {
            await assetOperation()
        }

        console.log(`FireMelon > Push > Will commit ${batchArray.length} batches`)
        let counter = 1
        try {
            if (batchArray.length > 0) {
                for (const batch of batchArray) {
                    console.log(`FireMelon > Push > Batch ${counter} of ${batchArray.length} > commit`);
                    await batch.commit();
                    console.log(`FireMelon > Push > Commit batch ${counter} of ${batchArray.length} done`);
                    counter++;
                }
            } else {
                console.log('FireMelon > Push > Nothing to push');
            }
        } catch (error) {
            //@ts-ignore
            throw new Error(error.message)
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
                console.log("FireMelon > Sync error > Retry sync");
                retry = false;
                try {
                    console.log("FireMelon > Sync > execute the retry");
                    await sync();
                    console.log("FireMelon > Sync > Retry sync successful > return true");
                    return true
                } catch (error) {
                    console.log("FireMelon > Sync failed");
                    console.error(error)
                    return false
                }
            } else {
                console.log("FireMelon > Sync failed");
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