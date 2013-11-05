class UserDatabaseDriver:
    def login(self, username, password):
        pass

    def logout(self):
        pass

    def profile(self, user_id=None):
        pass

    def update(self, user_id=None, **kwargs):
        pass

    def create(self, username, gender, birthday, introduction, avatar):
        pass

    def delete(self, user_id):
        pass

    def list(self):
        pass

class PinDatabaseDriver:
    def list(self, catalog=None):
        pass

    def create(self, title, content, board_id, img_data):
        pass

    def update(self, pin_id=None, **kwargs):
        pass

    def delete(self, pin_id):
        pass

    def get(self, pin_id):
        pass

class CommentDatabaseDriver:
    def list(self, pin_id=None):
        pass

    def create(self, pin_id, content):
        pass

    def update(self, pin_id, **kwargs):
        pass

    def delete(self, pin_id):
        pass

class FavoriteDatabaseDriver:
    def up(self, pin_id):
        pass

    def down(self, pin_id):
        pass

class FollowDatabaseDriver:
    def fo(self, username):
        pass

    def unfo(self, username):
        pass

    def list_following(self, username, page):
        pass

    def list_follower(self, username, page):
        pass

