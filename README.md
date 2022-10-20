# Firemelon

[![NPM version](https://img.shields.io/npm/v/firemelon)](https://www.npmjs.com/package/firemelon?activeTab=versions)
[![Test](https://github.com/AliAllaf/firemelon/workflows/Test/badge.svg)](https://github.com/AliAllaf/firemelon)

A simple way to sync between WatermelonDB and Firestore.

## Installation

Using npm :

```
$ npm install firemelon
```

Using yarn :

```
$ yarn add firemelon
```

In order to get this version of Firemelon up and running you'll need to create additional indexes in Firebase. The reason for this can be found in the pull Firebase queries that are executed. For each WaterMelon model the query will check whether the date - created or updated - is between 'now' and the last pull date, and in addition it will check wether the item has not been deleted. Doing so avoids retrieving items that were already deleted and thus do not have to be retrieved any longer.

## Improvement wish list
As in Firebase we pay for the amount of documents read we'd ideally get only the exact documents that we need so that there is no need for local filtering. In particular there are still two local filterings:
1. On the sessionId - each device gets a unique id so that we do not create/update/delete items locally that have been created by the device we're syncing from (why would we anyways?): filter for items with a different sessionId only.
2. On checking whether the item to be updated is not part of the create set already. 

These filterings are done locally because the FireStore doesn't allow for unequality comparison on more than 1 column. Impact - on FB pricing mainly - is likely to be minimal though.

## Compatibility

Firemelon works with both [@firebase/firestore](https://www.npmjs.com/package/@firebase/firestore) and [@react-native-firebase/firestore](https://www.npmjs.com/package/@react-native-firebase/firestore). The testing framework will only work with node version between 13 and 16, in particular it is not (yet) compatible with node version 17.

## Usage

```typescript
import { SyncFireMelon } from 'firemelon';

async () => {
    const syncable = SyncFireMelon(database, syncObject, firestore, () => sessionId, timefn(), {...watermelonSyncArguments});
    const syncable.synchronize();
};
```

-   **database** :
    The _WatermelonDB_ database to be synced.

-   **syncObject** :
    An object in which the synced collections and there options are

### Example:

```typescript
const syncObject = {
    // collections to sync
    todos: {
        // (optional)
        excludedFields: ['color', 'userId'],

        // To provide extra filters in queries. (optional)
        customQuery: firestore.collection('todos').where('color', '==', 'red'),
    },

    users: {},
};
```

-   **firestore** :
    The _Firestore_ module used in the app.

-   **sessionId** :
    A unique ID for each session to prevent the function from pulling its own pushed changes.

-   **timefn()** :

    A custom function to provide a date for the sync time.
    default is `new Date()`.

    This is an example of a more accurate way :

    ```typescript
    const timefn = () => firestore.FieldValue.serverTimestamp();
    ```
