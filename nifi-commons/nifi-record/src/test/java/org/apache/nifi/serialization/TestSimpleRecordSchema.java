/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.nifi.serialization;

import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.SchemaIdentifier;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertTrue;

public class TestSimpleRecordSchema {

    @Test
    public void testPreventsTwoFieldsWithSameAlias() {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("hello", RecordFieldType.STRING.getDataType(), null, set("foo", "bar")));
        fields.add(new RecordField("goodbye", RecordFieldType.STRING.getDataType(), null, set("baz", "bar")));

        try {
            new SimpleRecordSchema(fields);
            Assert.fail("Was able to create two fields with same alias");
        } catch (final IllegalArgumentException expected) {
        }
    }

    @Test
    public void testPreventsTwoFieldsWithSameName() {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("hello", RecordFieldType.STRING.getDataType(), null, set("foo", "bar")));
        fields.add(new RecordField("hello", RecordFieldType.STRING.getDataType()));

        try {
            new SimpleRecordSchema(fields);
            Assert.fail("Was able to create two fields with same name");
        } catch (final IllegalArgumentException expected) {
        }
    }

    @Test
    public void testPreventsTwoFieldsWithConflictingNamesAliases() {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("hello", RecordFieldType.STRING.getDataType(), null, set("foo", "bar")));
        fields.add(new RecordField("bar", RecordFieldType.STRING.getDataType()));

        try {
            new SimpleRecordSchema(fields);
            Assert.fail("Was able to create two fields with conflicting names/aliases");
        } catch (final IllegalArgumentException expected) {
        }
    }

    @Test
    public void testHashCodeAndEqualsWithSelfReferencingSchema() {
        {
            final SimpleRecordSchema schema = new SimpleRecordSchema(SchemaIdentifier.EMPTY);
            final List<RecordField> personFields = new ArrayList<>();
            personFields.add(new RecordField("name", RecordFieldType.STRING.getDataType()));
            personFields.add(new RecordField("sibling", RecordFieldType.RECORD.getRecordDataType(schema)));
            schema.setFields(personFields);

            schema.hashCode();
            assertTrue(schema.equals(schema));

            final SimpleRecordSchema secondSchema = new SimpleRecordSchema(SchemaIdentifier.EMPTY);
            secondSchema.setFields(personFields);
            assertTrue(schema.equals(secondSchema));
            assertTrue(secondSchema.equals(schema));
        }

        {
            final SimpleRecordSchema personSchema = new SimpleRecordSchema(SchemaIdentifier.EMPTY);
            final List<RecordField> personFields = new ArrayList<>();
            personFields.add(new RecordField("name", RecordFieldType.STRING.getDataType()));
            personFields.add(new RecordField("address", RecordFieldType.STRING.getDataType()));
            personFields.add(new RecordField("SIN", RecordFieldType.STRING.getDataType()));
            personFields.add(new RecordField("PersonalData", RecordFieldType.RECORD.getRecordDataType(personSchema)));
            personSchema.setFields(personFields);

            // Set up second schema making sure there are no shared references between schemas
            SimpleRecordSchema secondSchema = new SimpleRecordSchema(SchemaIdentifier.EMPTY);
            personFields.clear();
            personFields.add(new RecordField("name", RecordFieldType.STRING.getDataType()));
            personFields.add(new RecordField("address", RecordFieldType.STRING.getDataType()));
            personFields.add(new RecordField("SIN", RecordFieldType.STRING.getDataType()));
            personFields.add(new RecordField("PersonalData", RecordFieldType.RECORD.getRecordDataType(secondSchema)));
            secondSchema.setFields(personFields);

            assertTrue(personSchema.equals(secondSchema));
        }
    }



    @Test
    public void testSchemaNotEqualsCompareSelfRef() {

        // Test#1 : Field name differs (SIN vs. OAS)
        //
        {
            final SimpleRecordSchema personSchema = new SimpleRecordSchema(SchemaIdentifier.EMPTY);
            final List<RecordField> personFields = new ArrayList<>();
            personFields.add(new RecordField("name", RecordFieldType.STRING.getDataType()));
            personFields.add(new RecordField("address", RecordFieldType.STRING.getDataType()));
            personFields.add(new RecordField("OAS", RecordFieldType.STRING.getDataType())); // Different Name
            personFields.add(new RecordField("PersonalData", RecordFieldType.RECORD.getRecordDataType(personSchema)));
            personSchema.setFields(personFields);

            // Set up second schema making sure there are no shared references between schemas
            SimpleRecordSchema secondSchema = new SimpleRecordSchema(SchemaIdentifier.EMPTY);
            personFields.clear();
            personFields.add(new RecordField("name", RecordFieldType.STRING.getDataType()));
            personFields.add(new RecordField("address", RecordFieldType.STRING.getDataType()));
            personFields.add(new RecordField("SIN", RecordFieldType.STRING.getDataType()));
            personFields.add(new RecordField("PersonalData", RecordFieldType.RECORD.getRecordDataType(secondSchema)));
            secondSchema.setFields(personFields);

            assertTrue(!personSchema.equals(secondSchema));
            assertTrue(!secondSchema.equals(personSchema));
        }

        // Test#2 - All field names the same, one type is different
        {
            final SimpleRecordSchema personSchema = new SimpleRecordSchema(SchemaIdentifier.EMPTY);
            final List<RecordField> personFields = new ArrayList<>();
            personFields.add(new RecordField("name", RecordFieldType.STRING.getDataType()));
            personFields.add(new RecordField("address", RecordFieldType.STRING.getDataType()));
            personFields.add(new RecordField("SIN", RecordFieldType.INT.getDataType())); // Different Type
            personFields.add(new RecordField("PersonalData", RecordFieldType.RECORD.getRecordDataType(personSchema)));
            personSchema.setFields(personFields);

            // Set up second schema making sure there are no shared references between schemas
            SimpleRecordSchema secondSchema = new SimpleRecordSchema(SchemaIdentifier.EMPTY);
            personFields.clear();
            personFields.add(new RecordField("name", RecordFieldType.STRING.getDataType()));
            personFields.add(new RecordField("address", RecordFieldType.STRING.getDataType()));
            personFields.add(new RecordField("SIN", RecordFieldType.STRING.getDataType()));
            personFields.add(new RecordField("PersonalData", RecordFieldType.RECORD.getRecordDataType(secondSchema)));
            secondSchema.setFields(personFields);

            assertTrue(!personSchema.equals(secondSchema));
            assertTrue(!secondSchema.equals(personSchema));
        }


        // Test#3 - Add an additional simple Record Field in one schema
        {
            final SimpleRecordSchema personSchema = new SimpleRecordSchema(SchemaIdentifier.EMPTY);
            final List<RecordField> personFields = new ArrayList<>();
            personFields.add(new RecordField("name", RecordFieldType.STRING.getDataType()));
            personFields.add(new RecordField("address", RecordFieldType.STRING.getDataType()));
            personFields.add(new RecordField("SIN", RecordFieldType.STRING.getDataType()));
            personFields.add(new RecordField("Birthday", RecordFieldType.STRING.getDataType()));
            personFields.add(new RecordField("PersonalData", RecordFieldType.RECORD.getRecordDataType(personSchema)));
            personSchema.setFields(personFields);

            // Set up second schema making sure there are no shared references between schemas
            SimpleRecordSchema secondSchema = new SimpleRecordSchema(SchemaIdentifier.EMPTY);
            personFields.clear();
            personFields.add(new RecordField("name", RecordFieldType.STRING.getDataType()));
            personFields.add(new RecordField("address", RecordFieldType.STRING.getDataType()));
            personFields.add(new RecordField("SIN", RecordFieldType.STRING.getDataType()));
            personFields.add(new RecordField("PersonalData", RecordFieldType.RECORD.getRecordDataType(secondSchema)));
            secondSchema.setFields(personFields);

            assertTrue(!personSchema.equals(secondSchema));
            assertTrue(!secondSchema.equals(personSchema));
        }

        // Test#4 - Add an additional Record type Record Field in one schema
        {
            final SimpleRecordSchema personSchema = new SimpleRecordSchema(SchemaIdentifier.EMPTY);
            final List<RecordField> personFields = new ArrayList<>();
            personFields.add(new RecordField("name", RecordFieldType.STRING.getDataType()));
            personFields.add(new RecordField("address", RecordFieldType.STRING.getDataType()));
            personFields.add(new RecordField("SIN", RecordFieldType.STRING.getDataType()));
            personFields.add(new RecordField("Birthday", RecordFieldType.STRING.getDataType()));
            personFields.add(new RecordField("PersonalData", RecordFieldType.RECORD.getRecordDataType(personSchema)));
            personFields.add(new RecordField("PersonalData2", RecordFieldType.RECORD.getRecordDataType(personSchema)));
            personSchema.setFields(personFields);

            // Set up second schema making sure there are no shared references between schemas
            SimpleRecordSchema secondSchema = new SimpleRecordSchema(SchemaIdentifier.EMPTY);
            personFields.clear();
            personFields.add(new RecordField("name", RecordFieldType.STRING.getDataType()));
            personFields.add(new RecordField("address", RecordFieldType.STRING.getDataType()));
            personFields.add(new RecordField("SIN", RecordFieldType.STRING.getDataType()));
            personFields.add(new RecordField("PersonalData", RecordFieldType.RECORD.getRecordDataType(secondSchema)));
            secondSchema.setFields(personFields);

            assertTrue(!personSchema.equals(secondSchema));
            assertTrue(!secondSchema.equals(personSchema));
        }
    }


    private Set<String> set(final String... values) {
        final Set<String> set = new HashSet<>();
        for (final String value : values) {
            set.add(value);
        }
        return set;
    }

}
