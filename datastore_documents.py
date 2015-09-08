from datetime import datetime
import logging
import json

import os


GAE_RUNNING = False


try:
    from bson import ObjectId
    from gcloud import datastore
    from gcloud.datastore.key import Key
except ImportError:
    from google.appengine.ext import ndb
    from google.appengine.api.datastore_types import ResolveAppId
    from google.appengine.datastore import datastore_query
    FIXED_APP_ID = ResolveAppId(None)
    GAE_RUNNING = True

if GAE_RUNNING:
    KEY_CLASS = ndb.Key
else:
    KEY_CLASS = datastore.Key


GLOBAL_DEV_LIMIT = None
if os.environ.get('SERVER_SOFTWARE', '').startswith('Development'):
    GLOBAL_DEV_LIMIT = 5


class BaseField(object):
    if GAE_RUNNING:
        sub_class = ndb.Property

    def __new__(cls, *args, **kwargs):
        if GAE_RUNNING:
            if cls == ListField and len(args) > 0 and isinstance(args[0], ndb.Property):
                kwargs.update({'kind': args[0]._name,
                               'repeated': True})
                new_cls = type(cls.__name__, (type(args[0]), ), kwargs)(**kwargs)
            elif cls == ListField:
                kwargs.update({'repeated': True})
                new_cls = type(cls.__name__, (ndb.StringProperty, ), kwargs)(**kwargs)
            else:
                updated_kwargs = {}
                for k, v in kwargs.iteritems():
                    if k in ['name', 'indexed', 'repeated', 'required', 'default',
                             'choices', 'validator', 'verbose_name']:
                        updated_kwargs[k] = v
                if len(args) > 0:
                    updated_kwargs['kind'] = args[0]
                new_cls = type(cls.__name__, (cls.sub_class,), updated_kwargs)(**updated_kwargs)
            return new_cls
        else:
            new_cls = object.__new__(cls)
            if cls == ListField and len(args) > 0 and issubclass(type(args[0]), BaseField):
                new_cls.kind = type(args[0])
            elif len(args) > 0:
                new_cls.reference_key = args[0]

            return new_cls

    def __init__(self, *args, **kwargs):
        pass

    @classmethod
    def cast(self, value, follow_references=True):
        return value

    def uncast(self, value, follow_references=True):
        return value

    @classmethod
    def pre_save(self, value):
        return value


class BlobField(BaseField):
    if GAE_RUNNING:
        sub_class = ndb.BlobProperty

    # def __new__(cls, *args, **kwargs):
    #     if GAE_RUNNING:
    #         return type(cls.__name__, (ndb.StringProperty, cls), kwargs)
    #
    def cast(self, value, **kwargs):
        return unicode(value)


class StringField(BaseField):
    if GAE_RUNNING:
        sub_class = ndb.StringProperty

    @classmethod
    def cast(self, value, **kwargs):
        try:
            return unicode(value, 'utf-8')
        except:
            return value

class TextField(StringField):
    if GAE_RUNNING:
        sub_class = ndb.TextProperty

    def cast(self, value, **kwargs):
        #TODO: it is hack, we should refactor to use Blobs
        return (super(TextField, self).cast(value) or '')[:1048487]


class IntField(BaseField):
    if GAE_RUNNING:
        sub_class = ndb.IntegerProperty


class FloatField(BaseField):
    if GAE_RUNNING:
        sub_class = ndb.FloatProperty


class ReferenceField(BaseField):
    if GAE_RUNNING:
        sub_class = ndb.KeyProperty

    reference_key = ''

    @classmethod
    def cast(self, value, follow_references=True):
        if isinstance(value, (list, tuple)) and len(value) == 2:
            return KEY_CLASS(value[0], value[1], app=FIXED_APP_ID)
        elif isinstance(value, (int, str, unicode)):
            return KEY_CLASS(self.reference_key, int(value), app=FIXED_APP_ID)
        elif not value:
            return
        else:
            if isinstance(value, Document):
                return value._entity.key
            else:
                return value

    @classmethod
    def uncast(self, value, follow_references=True):
        if value and follow_references:
            values = datastore.get([value])
            if len(values) > 0:
                return _wrap_document(values[0])
        return value


class ListField(BaseField):

    @classmethod
    def cast(self, value, **kwargs):
        if not value:
            return []
        results = []
        for item in value:
            if isinstance(item, Document):
                results.append(item._entity.key)
            else:
                results.append(item)
        return results

    def uncast(self, value, follow_references=True):
        if not value:
            return []
        elif isinstance(value, (str, unicode)):
            value = json.loads(value)
        output_list = []
        for item in value:
            if hasattr(self, 'kind'):
                output_list.append(self.kind().uncast(item, follow_references))
            else:
                output_list.append(item)
        return output_list


class DateTimeField(BaseField):
    if GAE_RUNNING:
        sub_class = ndb.DateTimeProperty


class BooleanField(BaseField):
    if GAE_RUNNING:
        sub_class = ndb.BooleanProperty

class DictField(BaseField):
    if GAE_RUNNING:
        sub_class = ndb.JsonProperty

    @classmethod
    def cast(self, value, encode=False, **kwargs):
        if not value:
            return {}
        if isinstance(value, (str, unicode)):
            value = json.loads(value)
        return value

    def uncast(self, value, follow_references=True):
        if not value:
            return {}
        elif isinstance(value, (str, unicode)):
            value = json.loads(value)
        return value

    @classmethod
    def pre_save(self, value):
        return json.dumps(value)


def _wrap_document(entity, follow_references=True):
    return Document(entity=entity, follow_references=follow_references)


class QueryError(Exception):
    pass

class QuerySetManager(object):
    def __init__(self, entity, document):
        self._document = document
        self._entity = entity
        self.__post_processors = []
        self.__reset_query()

    def __fetch_all(self, limit=None, offset=None, local_results=None):
        results = []
        results_batch = []

        limit = limit or GLOBAL_DEV_LIMIT
        default_limit = limit or 200
        initial_limit = limit or 0
        default_offset = offset or 0

        if self.__results:
            results = self.__results

        extra_options = {}
        if GAE_RUNNING and self.__fields_projection:
            extra_options['projection'] = self.__fields_projection

        results_query = local_results or self.__results_query
        if results_query:
            while True:
                logging.debug('Start Query %s (fields: %s)' % (str(results_query.kind), str(self.__fields_projection)))

                results_batch = results_query.fetch(limit=default_limit,
                                                    offset=default_offset,
                                                    **extra_options)
                logging.debug('End Query %s' % str(results_query.filters))
                results_batch = list(results_batch)
                if results_batch:
                    results.extend(results_batch)
                if not results_batch or len(results_batch) < default_limit or len(results_batch) <= initial_limit:
                    break
                default_offset += default_limit
        for _post_processor in self.__post_processors:
            results = _post_processor(results)
        return results

    def __add_post_processor(self, func):
        self.__post_processors.append(func)

    def all(self, limit=None, follow_references=True):
        entities = self.__fetch_all(limit)

        results = []
        for entity in entities:
            results.append(_wrap_document(entity, follow_references))
            #yield self.__wrap_document(entity)
        return results

    def no_dereference(self, limit=None):
        return self.all(limit, follow_references=False)

    def delete(self):
        enitity_keys = []
        for entity in self.__fetch_all():
            enitity_keys.append(entity.key)
        if enitity_keys:
            ndb.delete_multi(enitity_keys)

    def distinct(self, field_name):
        entities = self.__fetch_all()

        unique_records = set()
        for entity in entities:
            doc = _wrap_document(entity)
            value = getattr(doc, field_name, None)
            if value not in unique_records:
                unique_records.add(value)
        return list(unique_records)

    def first(self):
        results = list(self.__fetch_all(limit=1))

        if len(results) > 0:
            return _wrap_document(results[0])
        else:
            return None

    def order_by(self, doc_property):
        if doc_property[0] == '-':
            field = doc_property[1:]
            reverse = True
        else:
            field = doc_property
            reverse = False
        if GAE_RUNNING:
            order = datastore_query.PropertyOrder.DESCENDING if reverse else datastore_query.PropertyOrder.ASCENDING
            order = datastore_query.PropertyOrder(field, order)
            self.__results_query = self.__results_query.order(order)
        else:

            self.__add_post_processor(lambda results: sorted(results, key=lambda x: getattr(x, field), reverse=reverse))

        return self

    def only(self, *keys):
        if keys:
            self.__fields_projection = keys
        return self

    def values_list(self, key):
        if self.__results:
            for entity in self.__results.fetch():
                yield getattr(_wrap_document(entity), key)

    def __parse_operator(self, key):
        key_parts = key.split('__', 1)
        key_name = key_parts[0]
        op_type = None
        if len(key_parts) > 1:
            op_type = key_parts[1]
        operator = None
        alt_value = None
        if not op_type:
            operator = '='
        elif op_type == 'gt':
            operator = '>'
        elif op_type == 'gte':
            operator = '>='
        elif op_type == 'lt':
            operator = '<'
        elif op_type == 'lte':
            operator = '<='
        elif op_type == 'ne':
            operator = '!='
        elif op_type == 'in':
            operator = 'in'
        elif op_type == 'exists':
            operator = '!='
            alt_value = ''
        if not operator:
            raise NotImplementedError('Query received unknown operator: %s' % key)
        return (key_name, operator, alt_value)

    def __get_keys(self, key_ids):
        keys = [KEY_CLASS(self._document._key_name, try_int(id), app=FIXED_APP_ID) for id in key_ids]
        if GAE_RUNNING:
            entities = ndb.get_multi(keys)
        else:
            entities = datastore.get(keys)
        return entities

    def __generate_query(self, query_filters, query_kwargs):
        if GAE_RUNNING:
            gae_filters = []
            for key_name, operator, v in query_filters:
                gae_filters.append(ndb.query.FilterNode(key_name, operator, v))
            query_filters = gae_filters

        results = None
        if GAE_RUNNING:
            results = self._entity.query(*query_filters, **query_kwargs)
        else:
            results = datastore.Query(self._entity.kind,
                                      filters=query_filters,
                                      projection=self.__fields_projection or [])
        return results

    def __call__(self, **kwargs):
        self.__reset_query()

        extra_filters_func = getattr(self._document, 'extra_filter', None)
        if extra_filters_func:
            kwargs.update(extra_filters_func())
        query_filters = []
        query_kwargs = {}

        main_in_conditions = []
        main_nin_conditions = []
        for k, v in kwargs.iteritems():
            key_name, operator, alt_v = self.__parse_operator(k)

            if alt_v is not None:
                v = alt_v

            if isinstance(v, Document):
                v = v._entity.key
            if isinstance(v, list):
                output_list = []
                for item in v:
                    if isinstance(item, Document):
                        output_list.append(item._entity.key)
                    elif key_name != 'id':
                        field = getattr(self._document, key_name)
                        if isinstance(field, ndb.KeyProperty):
                            if GAE_RUNNING:
                                output_list.append(KEY_CLASS(field.kind, try_int(item), app=FIXED_APP_ID))
                            else:
                                output_list.append(field.cast(item))
                        else:
                            output_list.append(item)
                    else:
                        output_list.append(item)

                if operator == 'in':
                    main_in_conditions.append({'key': key_name, 'values': output_list})
                    continue

                if operator == '!=':
                    main_nin_conditions.append({'key': key_name, 'values': output_list})
                    continue

                v = output_list
            if key_name == 'id':
                self.__results = self.__get_keys([v])
                continue
            else:
                query_filters.append((key_name, operator, v))

        if main_in_conditions:
            results = []
            for main_in_condition in main_in_conditions:
                if main_in_condition['key'] == 'id':
                    results = [self.__get_keys(main_in_condition['values'])]
                else:
                    results_local = []
                    for item in main_in_condition['values']:
                        local_filters = query_filters[:]
                        local_filters.append((main_in_condition['key'], '=', item))
                        query = self.__generate_query(local_filters, query_kwargs)

                        results_local += self.__fetch_all(local_results=query)
                    results.append(results_local)
            results_end = results[0][:]
            if len(results) > 1:
                results_end = [val for val in results[0] if val in results[1]]
            if len(results) > 2:
                results_end = [val for val in results_end if val in results[2]]
            self.__results = results_end
        elif main_nin_conditions:
            results = []
            for main_nin_condition in main_nin_conditions:
                if main_nin_condition['key'] == 'id':
                    results = [self.__get_keys(main_nin_condition['values'])]
                else:
                    results_local = []
                    for item in main_nin_condition['values']:
                        local_filters = query_filters[:]
                        local_filters.append((main_nin_condition['key'], '!=', item))
                        query = self.__generate_query(local_filters, query_kwargs)

                        results_local += self.__fetch_all(local_results=query)
                    results.append(results_local)
            results_end = results[0][:]
            if len(results) > 1:
                results_end = [val for val in results[0] if val in results[1]]
            if len(results) > 2:
                results_end = [val for val in results_end if val in results[2]]
            self.__results = results_end
        else:
            self.__results_query = self.__generate_query(query_filters, query_kwargs)
        return self

    def __reset_query(self):
        self.__results = []
        self.__results_query = None
        self.__fields_projection = None



ALL_DOCUMENTS = {}
ACTIVE_TRANSACTION = None
TRANSACTION_ENTITIES = []

class DocumentMetaClass(type):
    def __init__(cls, name, bases, classdict):
        super(DocumentMetaClass, cls).__init__(name, bases, classdict)

        attributes = {}
        for base_class in bases:
            attributes.update(base_class.__dict__)
        attributes.update(cls.__dict__)
        cls._fields = cls.collect_fields(attributes)

        cls._key_name = cls.__name__
        if GAE_RUNNING:
            cls._entity = type(cls._key_name, (ndb.Model,), cls._fields)()
        else:
            cls._entity = datastore.Entity(key=Key(cls._key_name))

        cls.objects = QuerySetManager(cls._entity, cls)
        ALL_DOCUMENTS[cls._key_name] = cls

    def collect_fields(cls, attributes):
        fields = {}
        for k, v in attributes.iteritems():
            if GAE_RUNNING:
                field_type = ndb.Property
            else:
                field_type = BaseField
            if issubclass(type(v), field_type):
                fields[k] = v
        fields['id'] = None
        return fields


class Document(object):

    _projection = ()

    _fields = {}
    _extra_filters_func = None
    _follow_references = True
    __metaclass__ = DocumentMetaClass

    def __new__(cls, **kwargs):
#        cls._fields = cls.__collect_fields()
        entity = kwargs.get('entity')
        follow_references = kwargs.get('follow_references', True)

        if entity:
            if GAE_RUNNING:
                cls._fields = entity._properties
                entity_cls = ALL_DOCUMENTS[entity._get_kind()]
            else:
                cls._fields = dict(entity)
                entity_cls = ALL_DOCUMENTS[entity.kind]
            entity_cls._entity = entity
            entity_cls._follow_references = follow_references
            return object.__new__(entity_cls)
        cls._follow_references = follow_references
        return object.__new__(cls)

    def __init__(self, entity=None, **kwargs):
        self._cached_fields = {}
        if entity:
            self._entity = entity
            self._entity._exclude_from_indexes = getattr(self, '_exclude_from_indexes', set())
        else:
            self.__init_entity(entity, **kwargs)
        # for k, v in kwargs.iteritems():
        #     setattr(self, k, v)

    def __init_entity(self, entity, **kwargs):
        if GAE_RUNNING:
            self._entity = type(self._key_name, (ndb.Model,), self._fields)(**kwargs)
        else:
            exclude_indexes = getattr(self, '_exclude_from_indexes', set())

            _id = str(ObjectId())
            self._entity = datastore.Entity(key=Key(self._key_name, _id),
                                            exclude_from_indexes=exclude_indexes)
            for k, v in kwargs.iteritems():
                setattr(self, k, v)

    def __setitem__(self, name, value):
        setattr(self, name, value)

    def delete(self):
        self._entity.key.delete()

    @classmethod
    def from_dict(cls, obj):
        return cls(**obj)

    def to_dict(self):
        dict_obj = {}
        for field in self._fields:
            obj = getattr(self, field)

            if isinstance(obj, KEY_CLASS):
                obj = (obj.kind, obj.id)
            elif isinstance(obj, list):
                output_obj = []
                for item in obj:
                    if isinstance(obj, KEY_CLASS):
                        output_obj.append(item.kind, item.id)
                obj = output_obj

            dict_obj[field] = obj
        return dict_obj

    @classmethod
    def commit_transactions(cls):
        global ACTIVE_TRANSACTION
        global TRANSACTION_ENTITIES
        batch = ACTIVE_TRANSACTION
        if batch:
            batch.commit()
            TRANSACTION_ENTITIES = []
        ACTIVE_TRANSACTION = None

    def save(self, transactional=False, **kwargs):
        for field_name, field_obj in self._fields.iteritems():
            if isinstance(field_obj, DictField):
                pre_save_func = getattr(field_obj, 'pre_save')
                if not pre_save_func:
                    continue

                value = getattr(self, field_name)
                if isinstance(value, dict):
                    self._entity[field_name] = pre_save_func(value)

        global ACTIVE_TRANSACTION
        global TRANSACTION_ENTITIES

        if transactional and not GAE_RUNNING:
            batch = ACTIVE_TRANSACTION
            if not batch:
                batch = datastore.Batch()#Transaction()
                #transaction.begin()
                ACTIVE_TRANSACTION = batch

            batch.put(self._entity)
            TRANSACTION_ENTITIES.append(self._entity)
            if len(TRANSACTION_ENTITIES) > 200:
                batch.commit()
                TRANSACTION_ENTITIES = []
                ACTIVE_TRANSACTION = None
        else:
            if GAE_RUNNING:
                self._entity.put()
            else:
                datastore.put([self._entity])

    def __getitem__(self, item):
        return getattr(self, item)

    def __getattribute__(self, item):
        result = None

        if item != '_fields' and item in object.__getattribute__(self, '_fields'):
            cached_fields = object.__getattribute__(self, '_cached_fields')
            if item in cached_fields:
                value = cached_fields[item]

                if value is not None and not isinstance(value, KEY_CLASS):
                    return value

            if GAE_RUNNING:
                if item == 'id':
                    result = self._entity.key.id()
                else:
                    value = getattr(self._entity, item)
                    if value == 'None':
                        value = None
                    if isinstance(value, list):
                        result = []
                        if self._follow_references:
                            if len(value) > 0 and isinstance(value[0], ndb.Key):
                                try:
                                    result = [_wrap_document(ent) for ent in ndb.get_multi(value)]
                                except Exception as e:
                                    logging.error('Wrong keys %s: %s' % (str(value), e))
                            else:
                                result = value
                        else:
                            if isinstance(value, list) and len(value)>0 and isinstance(value[0], (unicode, str)):
                                result = [item for item in value]
                            else:
                                result = [key.id() for key in value]
                    elif isinstance(value, ndb.Key):
                        if self._follow_references:
                            try:
                                result = value.get()
                            except Exception as e:
                                logging.error('Wrong key %s: %s' % (str(value), e))
                            result = _wrap_document(result)
                        else:
                            result = value.id()
                    else:
                        result = value
            else:
                if item == 'id':
                    result = self._entity.key.id or self._entity.key.name
                else:
                    value = self._entity.get(item)

                    if value == 'None':
                        value = None
                    if isinstance(value, datetime):
                         value = value.replace(tzinfo=None)

                    field = self._fields[item]
                    if item != 'account' and hasattr(field, 'uncast'):
                        result = field.uncast(value,
                                              follow_references=self._follow_references)
                    else:
                        result = value

            cached_fields[item] = result
        elif item == 'id':
            key = getattr(self._entity, 'key')
            if key:
                if isinstance(key.id, (int, long)):
                    result = key.id
                else:
                    result = key.id()
        else:
            val = object.__getattribute__(self, item)
            if val == 'None':
                val = None
            result = val

        return result

    def __setattr__(self, key, value):
        if key not in self._fields:
            self.__dict__[key] = value
        else:
            field = self._fields[key]
            if hasattr(field, 'cast'):
                casted_val = field.cast(value, follow_references=self._follow_references)
                self._entity[key] = casted_val
                self._cached_fields[key] = casted_val
            else:
                if isinstance(value, Document):
                    value = value._entity.key
                elif isinstance(value, list):
                    output_values = []
                    for val in value:
                        if isinstance(val, Document):
                            output_values.append(val._entity.key)
                    value = output_values
                if isinstance(field, ndb.KeyProperty) and isinstance(value, (str, unicode)):
                    setattr(self._entity, key, ndb.Key(field.kind, value))
                else:
                    setattr(self._entity, key, value)
                self._cached_fields[key] = value


class DynamicDocument(Document):
    pass


def gcloud_transactional(func):
    def wrapped_trx(self, *args, **kwargs):
        try:
            transaction = datastore.Transaction()
            transaction.begin()
            result = func(self, *args, **kwargs)
            transaction.commit()
            return result
        except Exception as e:
            logging.exception('Error in transaction (func %s), rolling bask: %s' %
                            (func.__name__, e))
            transaction.rollback()

    return wrapped_trx


if GAE_RUNNING:
    transaction = ndb.transactional
else:
    transaction = gcloud_transactional


def try_int(value):
    try:
        return int(value)
    except:
        return value