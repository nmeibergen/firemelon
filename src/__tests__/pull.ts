import * as firebase from '@firebase/testing';
import { SyncFireMelon } from '../firestoreSync';
import { SyncObj } from '../types/interfaces';
import newDatabase, { Todo } from '../utils/schema';
import timeout from '../utils/timeout';

const projectId = 'firemelon';
const sessionId = 'asojfbaoufasoinfaso';

function authedApp(auth: any) {
    return firebase.initializeTestApp({ projectId, auth }).firestore();
}

describe('Pull Created', () => {
    afterEach(async () => {
        await firebase.clearFirestoreData({ projectId });
        await Promise.all(firebase.apps().map((app) => app.delete()));
    });

    it('should pull created documents from Firestore to WatermelonDB', async () => {
        const app1 = authedApp({ uid: 'owner' });

        const firstDatabase = newDatabase();
        const secondDatabase = newDatabase();

        const firstMelonTodosRef = firstDatabase.collections.get<Todo>('todos');
        const secondMelonTodosRef = secondDatabase.collections.get<Todo>('todos');

        const obj: SyncObj = {
            todos: {},
            users: {}
        };

        await firstDatabase.write(async () => {
            await firstMelonTodosRef.create((todo: any) => {
                todo.text = 'todo 1';
            });
        });

        await new SyncFireMelon(firstDatabase, obj, app1, () => sessionId, () => new Date()).synchronize();

        const secondMelonTodoCollectionBefore = await secondMelonTodosRef.query().fetch();

        await timeout(500);

        expect(secondMelonTodoCollectionBefore.length).toBe(0);

        await new SyncFireMelon(secondDatabase, obj, app1, () => 'secondSessionId', () => new Date()).synchronize();

        const secondMelonTodoCollection = await secondMelonTodosRef.query().fetch();

        expect(secondMelonTodoCollection[0].text).toBe('todo 1');
    });

    it('will execute asset create operation if requested', async () => {
        const app1 = authedApp({ uid: 'owner' });

        const firstDatabase = newDatabase();
        const secondDatabase = newDatabase();

        const firstMelonTodosRef = firstDatabase.collections.get<Todo>('todos');
        const secondMelonTodosRef = secondDatabase.collections.get<Todo>('todos');

        const createAsset = jest.fn();
        const obj: SyncObj = {
            todos: {
                asset: {
                    push: {
                        create: jest.fn(),
                        update: jest.fn(),
                        delete: jest.fn()
                    },
                    pull: {
                        create: createAsset,
                        update: jest.fn(),
                        delete: jest.fn(),
                    }
                }
            },
            users: {}
        };

        await firstDatabase.write(async () => {
            await firstMelonTodosRef.create((todo: any) => {
                todo.text = 'todo 1';
            });
        });

        await new SyncFireMelon(firstDatabase, obj, app1, () => sessionId, () => new Date()).synchronize();

        const secondMelonTodoCollectionBefore = await secondMelonTodosRef.query().fetch();

        await timeout(500);

        await new SyncFireMelon(secondDatabase, obj, app1, () => 'secondSessionId', () => new Date()).synchronize();

        expect(createAsset).toBeCalledTimes(1);
    });
});

describe('Pull Updated', () => {
    afterEach(async () => {
        await firebase.clearFirestoreData({ projectId });
        await Promise.all(firebase.apps().map((app) => app.delete()));
    });

    it('should pull updated documents from Firestore to WatermelonDB', async () => {
        const app1 = authedApp({ uid: 'owner' });

        const firstDatabase = newDatabase();
        const secondDatabase = newDatabase();

        const firstMelonTodosRef = firstDatabase.collections.get<Todo>('todos');
        const secondMelonTodosRef = secondDatabase.collections.get<Todo>('todos');

        const obj: SyncObj = {
            todos: {},
            users: {},
        };

        await firstDatabase.write(async () => {
            await firstMelonTodosRef.create((todo: any) => {
                todo.text = 'todo 1';
            });
        });

        await new SyncFireMelon(firstDatabase, obj, app1, () => sessionId, () => new Date()).synchronize();

        await timeout(500);

        await new SyncFireMelon(secondDatabase, obj, app1, () => 'secondSessionId', () => new Date()).synchronize();

        const firstMelonTodoCollection = await firstMelonTodosRef.query().fetch();
        await firstDatabase.write(async () => {
            await firstMelonTodoCollection[0].update((todo: any) => {
                todo.text = 'updated todo';
            });
        });
        await new SyncFireMelon(firstDatabase, obj, app1, () => sessionId, () => new Date()).synchronize();

        await timeout(500);

        await new SyncFireMelon(secondDatabase, obj, app1, () => 'secondSessionId', () => new Date()).synchronize();

        const secondMelonTodoCollection = await secondMelonTodosRef.query().fetch();

        expect(secondMelonTodoCollection.length).toBe(1);

        expect(secondMelonTodoCollection[0].text).toBe('updated todo');
    });

    it('will execute asset update operation if requested', async () => {
        const app1 = authedApp({ uid: 'owner' });

        const firstDatabase = newDatabase();
        const secondDatabase = newDatabase();

        const firstMelonTodosRef = firstDatabase.collections.get<Todo>('todos');

        const updateAsset = jest.fn();
        const obj: SyncObj = {
            todos: {
                asset: {
                    push: {
                        create: jest.fn(),
                        update: jest.fn(),
                        delete: jest.fn()
                    },
                    pull: {
                        create: jest.fn(),
                        update: updateAsset,
                        delete: jest.fn(),
                    }
                }
            },
            users: {}
        };

        await firstDatabase.write(async () => {
            await firstMelonTodosRef.create((todo: any) => {
                todo.text = 'todo 1';
            });
        });

        await new SyncFireMelon(firstDatabase, obj, app1, () => sessionId, () => new Date()).synchronize();

        await timeout(500);

        await new SyncFireMelon(secondDatabase, obj, app1, () => 'secondSessionId', () => new Date()).synchronize();

        const firstMelonTodoCollection = await firstMelonTodosRef.query().fetch();
        await firstDatabase.write(async () => {
            await firstMelonTodoCollection[0].update((todo: any) => {
                todo.text = 'updated todo';
            });
        });
        await new SyncFireMelon(firstDatabase, obj, app1, () => sessionId, () => new Date()).synchronize();

        await timeout(500);

        await new SyncFireMelon(secondDatabase, obj, app1, () => 'secondSessionId', () => new Date()).synchronize();

        expect(updateAsset).toBeCalledTimes(1);
    })
});

describe('Pull Deleted', () => {
    afterEach(async () => {
        await firebase.clearFirestoreData({ projectId });
        await Promise.all(firebase.apps().map((app) => app.delete()));
    });

    it('should pull marked-as-deleted documents from Firestore to WatermelonDB and mark them as deleted', async () => {
        const app1 = authedApp({ uid: 'owner' });

        const firstDatabase = newDatabase();
        const secondDatabase = newDatabase();

        const firstMelonTodosRef = firstDatabase.collections.get<Todo>('todos');
        const secondMelonTodosRef = secondDatabase.collections.get<Todo>('todos');

        const obj: SyncObj = {
            todos: {},
            users: {}
        };

        await firstDatabase.write(async () => {
            await firstMelonTodosRef.create((todo: any) => {
                todo.text = 'todo 1';
            });
        });

        await new SyncFireMelon(firstDatabase, obj, app1, () => sessionId, () => new Date()).synchronize();

        await timeout(500);

        await new SyncFireMelon(secondDatabase, obj, app1, () => 'secondSessionId', () => new Date()).synchronize();

        const firstMelonTodoCollection = await firstMelonTodosRef.query().fetch();
        await firstDatabase.write(async () => {
            await firstMelonTodoCollection[0].markAsDeleted();
        });

        await new SyncFireMelon(firstDatabase, obj, app1, () => sessionId, () => new Date()).synchronize();

        await timeout(500);

        await new SyncFireMelon(secondDatabase, obj, app1, () => 'secondSessionId', () => new Date()).synchronize();

        const secondMelonTodoCollection = await secondMelonTodosRef.query().fetch();

        expect(secondMelonTodoCollection.length).toBe(0);
    });

    it('will execute asset delete operation if requested', async () => {
        const app1 = authedApp({ uid: 'owner' });

        const firstDatabase = newDatabase();
        const secondDatabase = newDatabase();

        const firstMelonTodosRef = firstDatabase.collections.get<Todo>('todos');

        const deleteAsset = jest.fn();
        const obj: SyncObj = {
            todos: {
                asset: {
                    push: {
                        create: jest.fn(),
                        update: jest.fn(),
                        delete: jest.fn()
                    },
                    pull: {
                        create: jest.fn(),
                        update: jest.fn(),
                        delete: deleteAsset,
                    }
                }
            },
            users: {}
        };

        await firstDatabase.write(async () => {
            await firstMelonTodosRef.create((todo: any) => {
                todo.text = 'todo 1';
            });
        });

        await new SyncFireMelon(firstDatabase, obj, app1, () => sessionId, () => new Date()).synchronize();

        await timeout(500);

        await new SyncFireMelon(secondDatabase, obj, app1, () => 'secondSessionId', () => new Date()).synchronize();

        const firstMelonTodoCollection = await firstMelonTodosRef.query().fetch();
        await firstDatabase.write(async () => {
            await firstMelonTodoCollection[0].markAsDeleted();
        });

        await new SyncFireMelon(firstDatabase, obj, app1, () => sessionId, () => new Date()).synchronize();

        await timeout(500);

        await new SyncFireMelon(secondDatabase, obj, app1, () => 'secondSessionId', () => new Date()).synchronize();

        expect(deleteAsset).toBeCalledTimes(1);
    })

});
