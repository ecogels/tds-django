from django.db.backends.base.creation import BaseDatabaseCreation


class DatabaseCreation(BaseDatabaseCreation):

    def create_test_db(self, *args, **kwargs):
        import os
        db_name = super().create_test_db()

        here = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))
        with self.connection.cursor() as cursor:
            for f in ['init.sql', 'clr.sql']:
                with open(f'{here}/sql/{f}', 'r') as file:
                    sql = file.read()
                    for s in sql.split('\nGO\n'):
                        cursor.execute(s)
        return db_name

    def sql_table_creation_suffix(self):
        """ a lot of tests expect case sensitivity """
        return 'COLLATE Latin1_General_100_CS_AS_SC '
