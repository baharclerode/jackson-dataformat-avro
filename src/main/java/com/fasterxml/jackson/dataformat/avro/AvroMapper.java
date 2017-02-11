package com.fasterxml.jackson.dataformat.avro;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;

import org.apache.avro.Schema;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.BeanProperty;
import com.fasterxml.jackson.databind.DeserializationConfig;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationConfig;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.jsontype.TypeDeserializer;
import com.fasterxml.jackson.databind.jsontype.TypeIdResolver;
import com.fasterxml.jackson.databind.jsontype.TypeResolverBuilder;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.fasterxml.jackson.databind.jsontype.impl.ClassNameIdResolver;
import com.fasterxml.jackson.databind.jsontype.impl.TypeDeserializerBase;
import com.fasterxml.jackson.dataformat.avro.schema.AvroSchemaGenerator;

/**
 * Convenience {@link AvroMapper}, which is mostly similar to simply
 * constructing a mapper with {@link AvroFactory}, but also adds little
 * bit of convenience around {@link AvroSchema} generation.
 * 
 * @since 2.5
 */
public class AvroMapper extends ObjectMapper
{
    private static final long serialVersionUID = 1L;

    public AvroMapper() {
        this(new AvroFactory());
    }

    public AvroMapper(AvroFactory f) {
        super(f);
        registerModule(new AvroModule());
        setDefaultTyping(new PolymorphicTypeResolverBuilder());
    }

    protected AvroMapper(ObjectMapper src) {
        super(src);
    }
    
    @Override
    public AvroMapper copy()
    {
        _checkInvalidCopy(AvroMapper.class);
        return new AvroMapper(this);
    }

    @Override
    public Version version() {
        return PackageVersion.VERSION;
    }

    @Override
    public AvroFactory getFactory() {
        return (AvroFactory) _jsonFactory;
    }
    
    /**
     * @since 2.5
     */
    public AvroSchema schemaFor(Class<?> type) throws JsonMappingException
    {
        AvroSchemaGenerator gen = new AvroSchemaGenerator();
        acceptJsonFormatVisitor(type, gen);
        return gen.getGeneratedSchema();
    }

    /**
     * @since 2.5
     */
    public AvroSchema schemaFor(JavaType type) throws JsonMappingException
    {
        AvroSchemaGenerator gen = new AvroSchemaGenerator();
        acceptJsonFormatVisitor(type, gen);
        return gen.getGeneratedSchema();
    }

    /**
     * Method for reading an Avro Schema from given {@link InputStream},
     * and once done (successfully or not), closing the stream.
     *
     * @since 2.6
     */
    public AvroSchema schemaFrom(InputStream in) throws IOException
    {
        try {
            return new AvroSchema(new Schema.Parser().setValidate(true)
                    .parse(in));
        } finally {
            in.close();
        }
    }

    /**
     * Convenience method for reading {@link AvroSchema} from given
     * encoded JSON representation.
     *
     * @since 2.6
     */
    public AvroSchema schemaFrom(String schemaAsString) throws IOException
    {
        return new AvroSchema(new Schema.Parser().setValidate(true)
                .parse(schemaAsString));
    }

    /**
     * Convenience method for reading {@link AvroSchema} from given
     * encoded JSON representation.
     *
     * @since 2.6
     */
    public AvroSchema schemaFrom(File schemaFile) throws IOException
    {
        return new AvroSchema(new Schema.Parser().setValidate(true)
                .parse(schemaFile));
    }

    public class PolymorphicTypeResolverBuilder implements TypeResolverBuilder<PolymorphicTypeResolverBuilder> {

        private Class<?> _defaultImpl;
        private boolean _isVisible;

        @Override
        public Class<?> getDefaultImpl() {
            return _defaultImpl;
        }

        @Override
        public TypeSerializer buildTypeSerializer(SerializationConfig config, JavaType baseType, Collection<NamedType> subtypes) {
            return null;
        }

        @Override
        public TypeDeserializer buildTypeDeserializer(DeserializationConfig config, JavaType baseType, Collection<NamedType> subtypes) {
            return new PolymorphicTypeDeserializer(baseType, _isVisible, _defaultImpl);
        }

        @Override
        public PolymorphicTypeResolverBuilder init(JsonTypeInfo.Id idType, TypeIdResolver res) {
            return this;
        }

        @Override
        public PolymorphicTypeResolverBuilder inclusion(JsonTypeInfo.As includeAs) {
            return this;
        }

        @Override
        public PolymorphicTypeResolverBuilder typeProperty(String propName) {
            return this;
        }

        @Override
        public PolymorphicTypeResolverBuilder defaultImpl(Class<?> defaultImpl) {
            _defaultImpl = defaultImpl;
            return this;
        }

        @Override
        public PolymorphicTypeResolverBuilder typeIdVisibility(boolean isVisible) {
            _isVisible = isVisible;
            return this;
        }
    }


    public class PolymorphicTypeDeserializer extends TypeDeserializerBase {

        protected PolymorphicTypeDeserializer(JavaType baseType, boolean typeIdVisible, Class<?> defaultImpl) {
            super(baseType, new ClassNameIdResolver(baseType, getTypeFactory()), null, typeIdVisible, defaultImpl);
        }

        protected PolymorphicTypeDeserializer(TypeDeserializerBase src, BeanProperty property) {
            super(src, property);
        }

        @Override
        public TypeDeserializer forProperty(BeanProperty prop) {
            // usually if it's null:
            return (prop == _property) ? this : new PolymorphicTypeDeserializer(this, prop);
        }

        @Override
        public JsonTypeInfo.As getTypeInclusion() {
            return JsonTypeInfo.As.EXTERNAL_PROPERTY;
        }

        /**
         * Method called when actual object is serialized as JSON Array.
         */
        @Override
        public Object deserializeTypedFromArray(JsonParser jp, DeserializationContext ctxt) throws IOException {
            return _deserialize(jp, ctxt);
        }

        /**
         * Method called when actual object is serialized as JSON Object
         */
        @Override
        public Object deserializeTypedFromObject(JsonParser jp, DeserializationContext ctxt) throws IOException {
            return _deserialize(jp, ctxt);
        }

        @Override
        public Object deserializeTypedFromScalar(JsonParser jp, DeserializationContext ctxt) throws IOException {
            return _deserialize(jp, ctxt);
        }

        @Override
        public Object deserializeTypedFromAny(JsonParser jp, DeserializationContext ctxt) throws IOException {
            return _deserialize(jp, ctxt);
        }

    /*
    /***************************************************************
    /* Internal methods
    /***************************************************************
     */

        /**
         * Method that handles type information wrapper, locates actual
         * subtype deserializer to use, and calls it to do actual
         * deserialization.
         */
        @SuppressWarnings("resource")
        protected Object _deserialize(JsonParser p, DeserializationContext ctxt) throws IOException
        {
            // 02-Aug-2013, tatu: May need to use native type ids
            if (p.canReadTypeId()) {
                Object typeId = p.getTypeId();
                if (typeId != null) {
                    JavaType actualType = null;
                    try {
                        actualType = _idResolver.typeFromId(ctxt, typeId.toString());
                    } catch (IllegalArgumentException e) {
                        // Could not find class, fall back to default
                    }
                    if (actualType != null) {
                        return _deserializeWithNativeTypeId(p, ctxt, typeId);
                    }
                }
            }
            JsonDeserializer<Object> deser = null;


            if (_property != null) {
                deser = ctxt.findContextualValueDeserializer(_baseType, _property);
            }
            if (deser == null) {
                deser = ctxt.findNonContextualValueDeserializer(_baseType);
            }
            if (deser == null) {
                ctxt.findRootValueDeserializer(_baseType);
            }
            if (deser != null) {
                return deser.deserialize(p, ctxt);
            }
            throw ctxt.reportMappingException("Could not find deserializer for %s", _baseType);

        }
    }
}
