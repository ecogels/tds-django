from django.db.models.query import QuerySet
from django.db import connections


_bulk = QuerySet.bulk_update


def bulk_update(self, objs, fields, batch_size=None):
    affected = 0
    if connections[self.db].vendor == 'sqlserver':
        if batch_size is not None and batch_size < 0:
            raise ValueError('Batch size must be a positive integer.')
        if not fields:
            raise ValueError('Field names must be given to bulk_update().')
        objs = tuple(objs)
        if any(obj.pk is None for obj in objs):
            raise ValueError('All bulk_update() objects must have a primary key set.')
        field_names = {self.model._meta.get_field(name): name for name in fields}
        if any(not f.concrete or f.many_to_many for f in field_names):
            raise ValueError('bulk_update() can only be used with concrete fields.')
        if any(f.primary_key for f in field_names):
            raise ValueError('bulk_update() cannot be used with primary key fields.')
        if not objs:
            return 0
        for obj in objs:
            obj._prepare_related_fields_for_save(
                operation_name="bulk_update", fields=[self.model._meta.get_field(name) for name in fields]
            )
        same_values = {}
        for field in field_names:
            first = ok = True
            value = None
            for o in objs:
                attr = getattr(o, field.attname)
                if not first and value != attr:
                    ok = False
                    break
                if first:
                    first = False
                    value = attr
            if ok:
                same_values[field.attname] = value
        if len(same_values):
            affected = self.filter(pk__in={o.pk for o in objs}).update(**same_values)
            fields = [v for k, v in field_names.items() if k.attname not in same_values]

    if len(fields):
        return affected + _bulk(self, objs, fields, batch_size)
    return affected


bulk_update.alters_data = True

setattr(QuerySet, 'bulk_update', bulk_update)
