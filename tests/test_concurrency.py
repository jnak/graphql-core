from graphql.type import (
    GraphQLField,
    GraphQLObjectType,
    GraphQLSchema,
    GraphQLString,
)

from graphql import graphql
import threading
from promise import dataloader, promise


REQUEST_GLOBALS = threading.local()


def viewer_id_resolver(root, info, **args):
    return REQUEST_GLOBALS.current_user_id


def promise_viewer_id_resolver(root, info, **args):
    return promise.Promise.resolve(None).then(lambda x: REQUEST_GLOBALS.current_user_id) 


class UserIdLoader(dataloader.DataLoader):
    def batch_load_fn(self, user_ids):
        return promise.Promise.resolve(user_ids)


user_id_loader = UserIdLoader()


def dataloader_viewer_id_resolver(root, info, **args):
    return user_id_loader.load(REQUEST_GLOBALS.current_user_id)


queryType = GraphQLObjectType(
    "Query",
    fields=lambda: {
        "viewerId": GraphQLField(
            GraphQLString,
            resolver=viewer_id_resolver,
        ),
        "promiseViewerUserId": GraphQLField(
            GraphQLString,
            resolver=promise_viewer_id_resolver,
        ),
        "dataloaderViewerUserId": GraphQLField(
            GraphQLString,
            resolver=dataloader_viewer_id_resolver,
        ),
    
    },
)

Schema = GraphQLSchema(query=queryType)


def handle_request(session, query, variables={}):
    # Authenticate requests and set global user id
    # https://django-globals.readthedocs.io/en/latest/#usage
    # https://flask.palletsprojects.com/en/1.1.x/appcontext/
    REQUEST_GLOBALS.current_user_id = session.get('userId')

    return graphql(Schema, query, variables)


def send_request(user_id, query):
    session = {"userId": user_id}
    i = 0
    while i < 1000:
        i += 1

        result = handle_request(session, query)
        if result.errors:
            raise Exception('Execution error', result.errors)
        if 'viewerId' not in result.data:
            raise Exception('Missing data', result.data)
        assert result.data['viewerId'] == user_id, \
            "request #{}: logged in user {} - actual user {}".format(str(i), user_id, result.data['viewerId'])


def simulate_concurrent_requests(query):
    threads = [threading.Thread(target=send_request, args=(user_id, query)) for user_id in ['1', '2']]

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()


def test_regular_field():
    simulate_concurrent_requests("""
        query {
            viewerId
        }
    """)


def test_promised_field():
    simulate_concurrent_requests("""
        query {
            viewerId: promiseViewerUserId
        }
    """)


def test_dataloader_field():
    simulate_concurrent_requests("""
        query {
            viewerId: dataloaderViewerUserId
        }
    """)


# Run this directly
# PyTest does not report exceptions happening in threads
if __name__ == '__main__':
    test_regular_field()
    test_promised_field()
    test_dataloader_field()
