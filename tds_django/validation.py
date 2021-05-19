from django.db.backends.base.validation import BaseDatabaseValidation
from django.core import checks


class DatabaseValidation(BaseDatabaseValidation):

    def check_field_type(self, field, field_type):
        errors = []

        if field.db_index and field_type.upper() in self.connection._limited_data_types:
            errors.append(
                checks.Warning(
                    '%s does not support a database index on %s columns.'
                    % (self.connection.display_name, field_type),
                    hint=(
                        "An index won't be created. Silence this warning if "
                        "you don't care about it."
                    ),
                    obj=field,
                    id='fields.W162',
                )
            )
        return errors
