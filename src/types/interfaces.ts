import { CollectionRef, FirestoreModule, Query } from './firestore';

export interface Item {
    id: string;
}

export interface AssetOptions {
    push: {
        create: (data: any) => Promise<void>
        update: (data: any) => Promise<void>
        delete: (data: any) => Promise<void>
    }
    pull: {
        create: (data: any) => Promise<void>
        update: (data: any) => Promise<void>
        delete: (data: any) => Promise<void>
    }
}

export interface SyncCollectionOptions {
    excludedFields?: string[]
    customPullQuery?: (db: FirestoreModule, collectionName: string) => Query
    customPushCollection?: (db: FirestoreModule, collectionName: string) => CollectionRef
    asset?: AssetOptions
}

export interface SyncObj {
    [collectionName: string]: SyncCollectionOptions;
}

export interface SyncTimestamp {
    syncTime: {
        toDate(): Date;
    };
}
