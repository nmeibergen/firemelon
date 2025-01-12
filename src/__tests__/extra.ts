import * as firebase from '@firebase/testing';
import { SyncFireMelon } from '../firestoreSync';
import { SyncObj } from '../types/interfaces';
import newDatabase, { Todo } from '../utils/schema';
import timeout from '../utils/timeout';
import { Model } from '@nozbe/watermelondb';

const projectId = 'firemelon';
const sessionId = 'asojfbaoufasoinfaso';

function authedApp(auth: any) {
    return firebase.initializeTestApp({ projectId, auth }).firestore();
}

describe('extra', () => {
    afterAll(async () => {
        await firebase.clearFirestoreData({ projectId });
        await Promise.all(firebase.apps().map((app) => app.delete()));
    });

    it('should not try to update created document', async () => {
        const app1 = authedApp({ uid: 'owner' });

        const firstDatabase = newDatabase();
        const secondDatabase = newDatabase();

        const firstMelonTodosRef = firstDatabase.collections.get<Todo>('todos');
        const secondMelonTodosRef = secondDatabase.collections.get<Todo>('todos');

        const obj: SyncObj = {
            todos: {},
            users: {}
        };

        let created: Model;

        await firstDatabase.write(async () => {
            created = await firstMelonTodosRef.create((todo: any) => {
                todo.text = 'todo 1';
            });
        });
        await new SyncFireMelon(firstDatabase, obj, app1, () => sessionId, () => new Date()).synchronize();

        await firstDatabase.write(async () => {
            await created.update((todo: any) => {
                todo.text = 'todo 2';
            });
        });
        await new SyncFireMelon(firstDatabase, obj, app1, () => sessionId, () => new Date()).synchronize();
        await timeout(500);

        await new SyncFireMelon(secondDatabase, obj, app1, () => 'secondSessionId', () => new Date()).synchronize();
        await timeout(500);

        const secondMelonTodoCol = await secondMelonTodosRef.query().fetch();

        expect(secondMelonTodoCol.length).toBe(1);

        expect(secondMelonTodoCol[0].text).toBe('todo 2');
    });
});
