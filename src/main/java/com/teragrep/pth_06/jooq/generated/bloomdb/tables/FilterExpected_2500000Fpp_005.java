/*
 * Teragrep Archive Datasource (pth_06)
 * Copyright (C) 2021-2024 Suomen Kanuuna Oy
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 *
 *
 * Additional permission under GNU Affero General Public License version 3
 * section 7
 *
 * If you modify this Program, or any covered work, by linking or combining it
 * with other code, such other code is not for that reason alone subject to any
 * of the requirements of the GNU Affero GPL version 3 as long as this Program
 * is the same Program as licensed from Suomen Kanuuna Oy without any additional
 * modifications.
 *
 * Supplemented terms under GNU Affero General Public License version 3
 * section 7
 *
 * Origin of the software must be attributed to Suomen Kanuuna Oy. Any modified
 * versions must be marked as "Modified version of" The Program.
 *
 * Names of the licensors and authors may not be used for publicity purposes.
 *
 * No rights are granted for use of trade names, trademarks, or service marks
 * which are in The Program if any.
 *
 * Licensee must indemnify licensors and authors for any liability that these
 * contractual assumptions impose on licensors and authors.
 *
 * To the extent this program is licensed as part of the Commercial versions of
 * Teragrep, the applicable Commercial License may apply to this file if you as
 * a licensee so wish it.
 */
package com.teragrep.pth_06.jooq.generated.bloomdb.tables;

import com.teragrep.pth_06.jooq.generated.bloomdb.Bloomdb;
import com.teragrep.pth_06.jooq.generated.bloomdb.Indexes;
import com.teragrep.pth_06.jooq.generated.bloomdb.Keys;
import com.teragrep.pth_06.jooq.generated.bloomdb.tables.records.FilterExpected_2500000Fpp_005Record;
import com.teragrep.pth_06.jooq.generated.journaldb.tables.Logfile;

import java.util.Arrays;
import java.util.List;

import javax.annotation.Generated;

import org.jooq.Field;
import org.jooq.ForeignKey;
import org.jooq.Identity;
import org.jooq.Index;
import org.jooq.Name;
import org.jooq.Record;
import org.jooq.Row3;
import org.jooq.Schema;
import org.jooq.Table;
import org.jooq.TableField;
import org.jooq.UniqueKey;
import org.jooq.impl.DSL;
import org.jooq.impl.TableImpl;
import org.jooq.types.ULong;

/**
 * This class is generated by jOOQ.
 */
@Generated(value = {
        "http://www.jooq.org", "jOOQ version:3.12.4"
},
        comments = "This class is generated by jOOQ"
)
@SuppressWarnings({
        "all", "unchecked", "rawtypes"
})
public class FilterExpected_2500000Fpp_005 extends TableImpl<FilterExpected_2500000Fpp_005Record> {

    private static final long serialVersionUID = 1626135712;

    /**
     * The reference instance of <code>bloomdb.filter_expected_2500000_fpp_005</code>
     */
    public static final FilterExpected_2500000Fpp_005 FILTER_EXPECTED_2500000_FPP_005 = new FilterExpected_2500000Fpp_005();

    /**
     * The class holding records for this type
     */
    @Override
    public Class<FilterExpected_2500000Fpp_005Record> getRecordType() {
        return FilterExpected_2500000Fpp_005Record.class;
    }

    /**
     * The column <code>bloomdb.filter_expected_2500000_fpp_005.id</code>.
     */
    public final TableField<FilterExpected_2500000Fpp_005Record, Integer> ID = createField(
            DSL.name("id"), org.jooq.impl.SQLDataType.INTEGER.nullable(false).identity(true), this, ""
    );

    /**
     * The column <code>bloomdb.filter_expected_2500000_fpp_005.partition_id</code>.
     */
    public final TableField<FilterExpected_2500000Fpp_005Record, ULong> PARTITION_ID = createField(
            DSL.name("partition_id"), org.jooq.impl.SQLDataType.BIGINTUNSIGNED.nullable(false), this, ""
    );

    /**
     * The column <code>bloomdb.filter_expected_2500000_fpp_005.filter</code>.
     */
    public final TableField<FilterExpected_2500000Fpp_005Record, byte[]> FILTER = createField(
            DSL.name("filter"), org.jooq.impl.SQLDataType.BLOB.defaultValue(org.jooq.impl.DSL.field("NULL", org.jooq.impl.SQLDataType.BLOB)), this, ""
    );

    /**
     * Create a <code>bloomdb.filter_expected_2500000_fpp_005</code> table reference
     */
    public FilterExpected_2500000Fpp_005() {
        this(DSL.name("filter_expected_2500000_fpp_005"), null);
    }

    /**
     * Create an aliased <code>bloomdb.filter_expected_2500000_fpp_005</code> table reference
     */
    public FilterExpected_2500000Fpp_005(String alias) {
        this(DSL.name(alias), FILTER_EXPECTED_2500000_FPP_005);
    }

    /**
     * Create an aliased <code>bloomdb.filter_expected_2500000_fpp_005</code> table reference
     */
    public FilterExpected_2500000Fpp_005(Name alias) {
        this(alias, FILTER_EXPECTED_2500000_FPP_005);
    }

    private FilterExpected_2500000Fpp_005(Name alias, Table<FilterExpected_2500000Fpp_005Record> aliased) {
        this(alias, aliased, null);
    }

    private FilterExpected_2500000Fpp_005(
            Name alias,
            Table<FilterExpected_2500000Fpp_005Record> aliased,
            Field<?>[] parameters
    ) {
        super(alias, null, aliased, parameters, DSL.comment(""));
    }

    public <O extends Record> FilterExpected_2500000Fpp_005(
            Table<O> child,
            ForeignKey<O, FilterExpected_2500000Fpp_005Record> key
    ) {
        super(child, key, FILTER_EXPECTED_2500000_FPP_005);
    }

    @Override
    public Schema getSchema() {
        return Bloomdb.BLOOMDB;
    }

    @Override
    public List<Index> getIndexes() {
        return Arrays
                .<Index>asList(
                        Indexes.FILTER_EXPECTED_2500000_FPP_005_PARTITION_ID,
                        Indexes.FILTER_EXPECTED_2500000_FPP_005_PRIMARY
                );
    }

    @Override
    public Identity<FilterExpected_2500000Fpp_005Record, Integer> getIdentity() {
        return Keys.IDENTITY_FILTER_EXPECTED_2500000_FPP_005;
    }

    @Override
    public UniqueKey<FilterExpected_2500000Fpp_005Record> getPrimaryKey() {
        return Keys.KEY_FILTER_EXPECTED_2500000_FPP_005_PRIMARY;
    }

    @Override
    public List<UniqueKey<FilterExpected_2500000Fpp_005Record>> getKeys() {
        return Arrays
                .<UniqueKey<FilterExpected_2500000Fpp_005Record>>asList(
                        Keys.KEY_FILTER_EXPECTED_2500000_FPP_005_PRIMARY,
                        Keys.KEY_FILTER_EXPECTED_2500000_FPP_005_PARTITION_ID
                );
    }

    @Override
    public List<ForeignKey<FilterExpected_2500000Fpp_005Record, ?>> getReferences() {
        return Arrays.<ForeignKey<FilterExpected_2500000Fpp_005Record, ?>>asList(Keys.FK_LARGEFILTER_PARTITION);
    }

    public Logfile logfile() {
        return new Logfile(this, Keys.FK_LARGEFILTER_PARTITION);
    }

    @Override
    public FilterExpected_2500000Fpp_005 as(String alias) {
        return new FilterExpected_2500000Fpp_005(DSL.name(alias), this);
    }

    @Override
    public FilterExpected_2500000Fpp_005 as(Name alias) {
        return new FilterExpected_2500000Fpp_005(alias, this);
    }

    /**
     * Rename this table
     */
    @Override
    public FilterExpected_2500000Fpp_005 rename(String name) {
        return new FilterExpected_2500000Fpp_005(DSL.name(name), null);
    }

    /**
     * Rename this table
     */
    @Override
    public FilterExpected_2500000Fpp_005 rename(Name name) {
        return new FilterExpected_2500000Fpp_005(name, null);
    }

    // -------------------------------------------------------------------------
    // Row3 type methods
    // -------------------------------------------------------------------------

    @Override
    public Row3<Integer, ULong, byte[]> fieldsRow() {
        return (Row3) super.fieldsRow();
    }
}
