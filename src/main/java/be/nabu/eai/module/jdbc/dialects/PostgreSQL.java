/*
* Copyright (C) 2016 Alexander Verbruggen
*
* This program is free software: you can redistribute it and/or modify
* it under the terms of the GNU Lesser General Public License as published by
* the Free Software Foundation, either version 3 of the License, or
* (at your option) any later version.
*
* This program is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
* GNU Lesser General Public License for more details.
*
* You should have received a copy of the GNU Lesser General Public License
* along with this program. If not, see <https://www.gnu.org/licenses/>.
*/

package be.nabu.eai.module.jdbc.dialects;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URI;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import be.nabu.eai.repository.EAIRepositoryUtils;
import be.nabu.libs.property.ValueUtils;
import be.nabu.libs.property.api.Value;
import be.nabu.libs.services.api.ServiceException;
import be.nabu.libs.services.jdbc.JDBCUtils;
import be.nabu.libs.services.jdbc.api.SQLDialect;
import be.nabu.libs.types.DefinedTypeResolverFactory;
import be.nabu.libs.types.api.ComplexContent;
import be.nabu.libs.types.api.ComplexType;
import be.nabu.libs.types.api.DefinedType;
import be.nabu.libs.types.api.Element;
import be.nabu.libs.types.api.SimpleType;
import be.nabu.libs.types.api.Type;
import be.nabu.libs.types.base.Duration;
import be.nabu.libs.types.properties.CollectionNameProperty;
import be.nabu.libs.types.properties.DefaultValueProperty;
import be.nabu.libs.types.properties.ForeignKeyProperty;
import be.nabu.libs.types.properties.FormatProperty;
import be.nabu.libs.types.properties.GeneratedProperty;
import be.nabu.libs.types.properties.IndexedProperty;
import be.nabu.libs.types.properties.MinOccursProperty;
import be.nabu.libs.types.properties.NameProperty;
import be.nabu.libs.types.properties.PrimaryKeyProperty;
import be.nabu.libs.types.properties.UniqueProperty;
import be.nabu.libs.types.utils.DateUtils;
import be.nabu.libs.types.utils.DateUtils.Granularity;

public class PostgreSQL implements SQLDialect {

	private Logger logger = LoggerFactory.getLogger(getClass());
	// postgresql does _not_ expand the * for count queries which end up being a ton faster. had a 5s count query reduced to 200ms even though the explain cost "only" decreased from 150.000 to 50.000
	@Override
	public String getTotalCountQuery(String query) {
		return SQLDialect.getDefaultTotalCountQuery(query, true);
	}
	
	@Override
	public String getSQLNullName(Element<?> element) {
		String name = SQLDialect.super.getSQLName(element);
		boolean isList = element.getType().isList(element.getProperties());
		// postgresql has a name like "uuid", but also "uuid_array"
		// https://www.javadoc.io/doc/org.postgresql/postgresql/9.4.1208.jre6/constant-values.html
		// in the past they allowed you to pass in "uuid" even though you were setting an array, but in recent versions this started erroring out with a "can't cast uuid to uuid[]" exception
		// note that this is for setting a null value!
		if (isList) {
			name += "_array";
		}
		return name;
	}

	@Override
	public String getSQLName(Class<?> instanceClass) {
		if (UUID.class.isAssignableFrom(instanceClass)) {
			return "uuid";
		}
		else {
			return SQLDialect.super.getSQLName(instanceClass);
		}
	}
	
	@Override
	public Integer getDefaultPort() {
		return 5432;
	}

	@Override
	public String rewrite(String sql, ComplexType input, ComplexType output) {
		Pattern pattern = Pattern.compile("(?<!:)[:$][\\w$]+(?!::)(\\b|$|\\Z|\\z)");
		Matcher matcher = pattern.matcher(sql);
		StringBuilder result = new StringBuilder();
		int last = 0;
		while (matcher.find()) {
			if (matcher.end() > last) {
				result.append(sql.substring(last, matcher.end()));
			}
			String name = matcher.group().substring(1);
			Element<?> element = input.get(name);
			if (element != null && element.getType() instanceof SimpleType) {
				SimpleType<?> type = (SimpleType<?>) element.getType();
				String postgreType = null;
				boolean isList = element.getType().isList(element.getProperties());
				if (UUID.class.isAssignableFrom(type.getInstanceClass())) {
					postgreType = "uuid";
				}
				else if (Date.class.isAssignableFrom(type.getInstanceClass())) {
					String format = ValueUtils.getValue(FormatProperty.getInstance(), element.getProperties());
					Granularity granularity = format == null ? Granularity.TIMESTAMP : DateUtils.getGranularity(format);
					switch(granularity) {
						case DATE: postgreType = "date"; break;
						case TIME: postgreType = "time"; break;
						default: postgreType = "timestamp";
					}
				}
				else if (Duration.class.isAssignableFrom(type.getInstanceClass())) {
					postgreType = "interval";
				}
				else if (Boolean.class.isAssignableFrom(type.getInstanceClass())) {
					postgreType = "boolean";
				}
				// if we have a list, we want to always set a type, because suppose you have a text field and you do this:
				// where :value is null or my_field = any(:value)
				// this does _not_ work if the value is null and nothing is done explicitly, you get "postgresql op ANY/ALL (array) requires array on right side"
				// it does work however if we do this
				// where :value is null or my_field = any(:value::text[])
				// note that this does not seem to work with integers, if the above is the exact same scenario but with integer[] you get: ERROR: cannot cast type integer to integer[]
				else if (isList) {
					postgreType = getPredefinedSQLType(type.getInstanceClass());
				}
				if (postgreType != null) {
					result.append("::").append(postgreType);
					if (isList) {
						result.append("[]");
					}
				}
			}
			last = matcher.end();
		}
		if (last < sql.length()) {
			result.append(sql.substring(last, sql.length()));
		}
		String rewritten = result.toString();
		// replace in () with =any(), only if it contains a variable
		rewritten = rewritten.replaceAll("([\\s]+)in[\\s]*\\([\\s]*:", "$1= any(:");
		logger.trace("Rewrote '{}'\n{}", new Object[] { sql, rewritten });
		return rewritten;
	}

	@Override
	public String limit(String sql, Long offset, Integer limit) {
		if (offset != null) {
			sql = sql + " OFFSET " + offset;
		}
		if (limit != null) {
			sql = sql + " LIMIT " + limit;
		}
		return sql;
	}

	@Override
	public String buildCreateSQL(ComplexType type, boolean compact) {
		StringBuilder builder = new StringBuilder();
		for (Element<?> child : JDBCUtils.getFieldsInTable(type)) {
			Value<Boolean> generatedProperty = child.getProperty(GeneratedProperty.getInstance());
			if (generatedProperty != null && generatedProperty.getValue() != null && generatedProperty.getValue()) {
				String seqName = "seq_" + EAIRepositoryUtils.uncamelify(getName(type.getProperties())) + "_" + EAIRepositoryUtils.uncamelify(child.getName()); 
				builder.append("create sequence ").append(seqName).append(";\n");
			}
		}
		builder.append("create table " + EAIRepositoryUtils.uncamelify(getName(type.getProperties())) + " (" + (compact ? "" : "\n"));
		boolean first = true;
		for (Element<?> child : JDBCUtils.getFieldsInTable(type)) {
			if (first) {
				first = false;
			}
			else {
				builder.append("," + (compact ? " " : "\n"));
			}
			
			// if we have a complex type, generate an id field that references it
			if (child.getType() instanceof ComplexType) {
				builder.append((compact ? "" : "\t") + EAIRepositoryUtils.uncamelify(child.getName()) + "_id uuid");
			}
			// differentiate between dates
			else if (Date.class.isAssignableFrom(((SimpleType<?>) child.getType()).getInstanceClass())) {
				Value<String> property = child.getProperty(FormatProperty.getInstance());
				String format = property == null ? "dateTime" : property.getValue();
				Granularity granularity = format == null ? Granularity.TIMESTAMP : DateUtils.getGranularity(format);
				if (format.equals("dateTime")) {
					format = "timestamp";
				}
				else if (granularity == Granularity.TIME) {
					format = "time";
				}
				else if (granularity == Granularity.DATE) {
					format = "date";
				}
				else if (!format.equals("date") && !format.equals("time")) {
					format = "timestamp";
				}
				builder.append((compact ? "" : "\t") + EAIRepositoryUtils.uncamelify(child.getName())).append(" ").append(format);
			}
			else {
				builder.append((compact ? "" : "\t") + EAIRepositoryUtils.uncamelify(child.getName())).append(" ")
					.append(getPredefinedSQLType(((SimpleType<?>) child.getType()).getInstanceClass()));
			}
			
			boolean isList = child.getType().isList(child.getProperties());
			if (isList) {
				builder.append("[]");
			}
			
			Value<Boolean> primaryKeyProperty = child.getProperty(PrimaryKeyProperty.getInstance());
			Value<Boolean> generatedProperty = child.getProperty(GeneratedProperty.getInstance());
			
			if (primaryKeyProperty != null && primaryKeyProperty.getValue() != null && primaryKeyProperty.getValue()) {
				builder.append(" primary key");
			}
			else {
				if (child.getName().equals("id")) {
					builder.append(" primary key");
				}
				else {
					Integer value = ValueUtils.getValue(MinOccursProperty.getInstance(), child.getProperties());
					boolean mandatory = false;
					if (value == null || value > 0 || (generatedProperty != null && generatedProperty.getValue() != null && generatedProperty.getValue())) {
						builder.append(" not null");
						mandatory = true;
					}
					Value<String> defaultValue = child.getProperty(DefaultValueProperty.getInstance());
					if (defaultValue != null && defaultValue.getValue() != null && !defaultValue.getValue().trim().isEmpty()) {
						builder.append(" default " + defaultValue.getValue());
					}
					// for mandatory boolean values, we automatically insert "default false", this makes it easier to add mandatory boolean later on with alter scripts
					else if (mandatory && Boolean.class.isAssignableFrom(((SimpleType<?>) child.getType()).getInstanceClass())) {
						builder.append(" default false");
					}
				}
			}
			
			Value<String> foreignKey = child.getProperty(ForeignKeyProperty.getInstance());
			if (foreignKey != null) {
				String[] split = foreignKey.getValue().split(":");
				if (split.length == 2) {
					DefinedType resolve = DefinedTypeResolverFactory.getInstance().getResolver().resolve(split[0]);
					String referencedName = ValueUtils.getValue(CollectionNameProperty.getInstance(), resolve.getProperties());
					if (referencedName == null) {
						referencedName = resolve.getName();
					}
					builder.append(" references " + EAIRepositoryUtils.uncamelify(referencedName) + "(" + split[1] + ")");
				}
			}
			// if we have a supertype, it has a field by the exact same name which is also a primary key, we set a foreign key
			else if (primaryKeyProperty != null && primaryKeyProperty.getValue() != null && primaryKeyProperty.getValue()) {
				Type superType = type.getSuperType();
				if (superType instanceof ComplexType) {
					Element<?> element = ((ComplexType) superType).get(child.getName());
					if (element != null) {
						Value<Boolean> superPrimaryKey = element.getProperty(PrimaryKeyProperty.getInstance());
						String superName = ValueUtils.getValue(CollectionNameProperty.getInstance(), superType.getProperties());
						if (superName == null) {
							superName = superType.getName();
						}
						if (superPrimaryKey != null && superPrimaryKey.getValue() != null && superPrimaryKey.getValue()) {
							builder.append(" references " + EAIRepositoryUtils.uncamelify(superName) + "(" + child.getName() + ")");
						}
					}
				}
			}
			
			Value<Boolean> property = child.getProperty(UniqueProperty.getInstance());
			if (property != null && property.getValue()) {
				builder.append(" unique");
			}
			
			if (generatedProperty != null && generatedProperty.getValue() != null && generatedProperty.getValue()) {
				String seqName = "seq_" + EAIRepositoryUtils.uncamelify(getName(type.getProperties())) + "_" + EAIRepositoryUtils.uncamelify(child.getName());
				builder.append(" default nextval('" + seqName + "')");
			}
		}
		builder.append((compact ? "" : "\n") + ");");
		// create indexes
		builder.append("\n");
		for (Element<?> child : JDBCUtils.getFieldsInTable(type)) {
			Value<Boolean> indexedProperty = child.getProperty(IndexedProperty.getInstance());
			if (indexedProperty != null && indexedProperty.getValue() != null && indexedProperty.getValue()) {
				String tableName = EAIRepositoryUtils.uncamelify(getName(type.getProperties()));
				String columnName = EAIRepositoryUtils.uncamelify(child.getName());
				String seqName = "idx_" + tableName + "_" + columnName; 
				builder.append("create index ").append(seqName).append(" on " + tableName + "(" + columnName + ")").append(";\n");
			}
		}
		return builder.toString();
	}

	
	public static String getPredefinedSQLType(Class<?> instanceClass) {
		if (String.class.isAssignableFrom(instanceClass) || char[].class.isAssignableFrom(instanceClass) || URI.class.isAssignableFrom(instanceClass) || instanceClass.isEnum()) {
			// best practice to use application level limits on text
			return "text";
		}
		else if (Duration.class.isAssignableFrom(instanceClass)) {
			return "interval";
		}
		else if (byte[].class.isAssignableFrom(instanceClass)) {
			return "bytea";
		}
		else if (Integer.class.isAssignableFrom(instanceClass)) {
			return "integer";
		}
		else if (Long.class.isAssignableFrom(instanceClass) || BigInteger.class.isAssignableFrom(instanceClass)) {
			return "bigint";
		}
		else if (Double.class.isAssignableFrom(instanceClass) || BigDecimal.class.isAssignableFrom(instanceClass)) {
			return "decimal";
		}
		else if (Float.class.isAssignableFrom(instanceClass)) {
			return "decimal";
		}
		else if (Short.class.isAssignableFrom(instanceClass)) {
			return "smallint";
		}
		else if (Boolean.class.isAssignableFrom(instanceClass)) {
			return "boolean";
		}
		else if (UUID.class.isAssignableFrom(instanceClass)) {
			return "uuid";
		}
		else if (Date.class.isAssignableFrom(instanceClass)) {
			return "timestamp";
		}
		else {
			return null;
		}
	}
	
	public static String getName(Value<?>...properties) {
		String value = ValueUtils.getValue(CollectionNameProperty.getInstance(), properties);
		if (value == null) {
			value = ValueUtils.getValue(NameProperty.getInstance(), properties);
		}
		return value;
	}
	
	
	@Override
	public boolean supportNumericGroupBy() {
		return true;
	}

	@Override
	public String buildInsertSQL(ComplexContent content, boolean compact) {
		StringBuilder keyBuilder = new StringBuilder();
		StringBuilder valueBuilder = new StringBuilder();
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		formatter.setTimeZone(TimeZone.getTimeZone("UTC"));
		Date date = new Date();
		for (Element<?> element : JDBCUtils.getFieldsInTable(content.getType())) {
			if (element.getType() instanceof SimpleType) {
				Class<?> instanceClass = ((SimpleType<?>) element.getType()).getInstanceClass();
				if (!keyBuilder.toString().isEmpty()) {
					keyBuilder.append(",").append(compact ? " " : "\n\t");
					valueBuilder.append(",").append(compact ? " " : "\n\t");
				}
				keyBuilder.append(EAIRepositoryUtils.uncamelify(element.getName()));
				Object value = content.get(element.getName());
				Integer minOccurs = ValueUtils.getValue(MinOccursProperty.getInstance(), element.getProperties());
				// if there is no value but it is mandatory, try to generate one
				if (value == null && minOccurs != null && minOccurs > 0) {
					if (UUID.class.isAssignableFrom(instanceClass)) {
						value = UUID.randomUUID();
					}
					else if (Date.class.isAssignableFrom(instanceClass)) {
						value = date;
					}
					else if (Number.class.isAssignableFrom(instanceClass)) {
						value = 0;
					}
					else if (Boolean.class.isAssignableFrom(instanceClass)) {
						value = false;
					}
				}
				if (value == null) {
					valueBuilder.append("null");
				}
				else {
					boolean closeQuote = false;
					if (Date.class.isAssignableFrom(instanceClass)) {
						valueBuilder.append("timestamp '").append(formatter.format(value)).append("'");
					}
					else {
						if (URI.class.isAssignableFrom(instanceClass) || String.class.isAssignableFrom(instanceClass) || UUID.class.isAssignableFrom(instanceClass)) {
							valueBuilder.append("'");
							closeQuote = true;
							value = value.toString().replace("'", "''");
						}
						valueBuilder.append(value.toString());
						if (closeQuote) {
							valueBuilder.append("'");
						}
					}
				}
			}
		}
		return "insert into " + EAIRepositoryUtils.uncamelify(getName(content.getType().getProperties())) + " (" + (compact ? "" : "\n\t") + keyBuilder.toString() + (compact ? "" : "\n") + ") values (" + (compact ? "" : "\n\t") + valueBuilder.toString() + (compact ? "" : "\n") + ");";
	}

	@Override
	public Exception wrapException(SQLException e) {
		// unwind
		while (e.getNextException() != null) {
			e = e.getNextException();
		}
		
		// we have a unique constraint issue
		if (e.getMessage().indexOf("duplicate key value violates unique constraint") >= 0) {
			String field = null;
			int firstIndex = e.getMessage().indexOf('"');
			if (firstIndex > 0) {
				int secondIndex = e.getMessage().indexOf('"', firstIndex + 1);
				if (secondIndex > 0) {
					field = e.getMessage().substring(firstIndex + 1, secondIndex);
				}
			}
			ServiceException serviceException = new ServiceException("JDBC-UNIQUE-VIOLATION", "Unique constraint violation", e);
			serviceException.setDescription("Unique constraint violation for " + (field == null ? "unknown field" : "'" + field + "'"));
			return serviceException;
		}
		
		return null;
	}
	
}
