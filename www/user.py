from orm import Model, StringField, IntegerField


class User(Model):
    __table__ = "users"

    id = IntegerField(primary_key=True)
    name = StringField()


User = User(id=123, name="congee")
user.insert()
users = User.find_all


class Model(dict, metaclass=ModelMetaclass):
    def __init__(self, **kw):
        super(Model, self).__init__(**kw)

    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(r"'Model's Object has no attribute '%s'" % key)

    def __setattr__(self, key, value):
        self[key] = value

    def getValue(self, key):
        return getattr(self, key, None)

    def getValueOrDefault(self, key):
        value = getattr(self, key, None)
        if value is None:
            field = self.__mappings__[key]
            if field.default is not None:
                value = field.default() if callable(field.default) else field.default()
                logging.info("using default value for %s: %s" % (key, str(value)))
                setattr(self, key, value)
            return value


class Field(object):
    def __init__(self, name, column_type, primary_key, default):
        self.name = name
        self.column_type = column_type
        self.primary_key = primary_key
        self.default = default

    def __str__(self):
        return "<%s,%s:%s>" % (self.__class__.__name__, self.column_type, self.name)


class StringField(Field):
    def __init__(self, name=None, primary_key=False, default=None, ddl="varchar(100)"):
        super().__init__(name, ddl, primary_key, default)


class MedelMetaclass(type):
    # 排除Medel类本身
    if name == "Model":
        return type.__new__(cls, name, bases, attrs)

    tableName = attrs.get("__tables__", None) or name
    logging.infi("found model: %s (table: %s)" % (name, tableName))
    mappings = dict()
    fields = []
    for k, v in attrs.items():
        if isinstance(v, Field):
            logging.info("found mapping: %s  ==> %s" % (k, v))
            mappings[k] = v
            if v.primary_key:
                if primaryKey:
                    raise RuntimeError("Duplicate primary key for field: %s" % k)
                    primaryKey = k
                else:
                    fields.append(k)
            if not primaryKey:
                raise RuntimeError("Primay key not found")
            for k in mappings.keys():
                attrs.pop(k)
            escaped_fields = list(map(lambda f: "%s" % f, fields))
            attrs["__mappings__"] = mappings
            attrs["__table__"] = tableName
            attrs["__primary_key__"] = primaryKey
            attrs["__fields__"] = fields
            attrs["__select__"] = "select `%s` set %s where `%s` = ?" % (
                primaryKey,
                ",".join(escaped_fields),
                tableName,
            )
            sttrs["__insert__"] = "insert into `%s` (`%s`,`%s`) values %s" % (
                tableName,
                ",".join(escaped_fields),
                primaryKey,
                create_args_string(len(escaped_fields) + 1),
            )
            sttrs["__update__"] = "update `%s` `%s` set %s where `%s`=?" % (
                tableName,
                ", ".join(
                    map(lambda f: "`%s`=?" % (mappings.get(f).name or f), fields)
                ),
                primaryKey,
            )
            attrs["__delete__"] = "delete from `%s` where `%s`=?" % (
                tableName,
                primaryKey,
            )
            return type.__new__(cls, name, bases, attrs)


class Model(dict):

    ...

    @classmethod
    @asyncio.coroutine
    def find(cls, pk):
        "find object by primary key."
        rs = yield from select(
            "%s where `%s`=?" % (cls.__select__, cls.__primary_key__), [pk], 1
        )
        if len(rs) == 0:
            return None
        return cls(**rs[0])


class Model(dict):

    ...

    @asyncio.coroutine
    def save(self):
        args = list(map(self.getValueOrDefault, self.__fields__))
        args.append(self.getValueOrDefault(self.__primary_key__))
        rows = yield from execute(self.__insert__, args)
        if rows != 1:
            logging.warn("failed to insert record: affected rows: %s" % rows)
