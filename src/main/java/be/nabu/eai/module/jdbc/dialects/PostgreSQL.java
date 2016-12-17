package be.nabu.eai.module.jdbc.dialects;

import java.net.URI;
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
import be.nabu.libs.services.jdbc.api.SQLDialect;
import be.nabu.libs.types.DefinedTypeResolverFactory;
import be.nabu.libs.types.TypeUtils;
import be.nabu.libs.types.api.ComplexContent;
import be.nabu.libs.types.api.ComplexType;
import be.nabu.libs.types.api.DefinedType;
import be.nabu.libs.types.api.Element;
import be.nabu.libs.types.api.SimpleType;
import be.nabu.libs.types.properties.CollectionNameProperty;
import be.nabu.libs.types.properties.ForeignKeyProperty;
import be.nabu.libs.types.properties.FormatProperty;
import be.nabu.libs.types.properties.MinOccursProperty;
import be.nabu.libs.types.properties.NameProperty;
import be.nabu.libs.types.properties.UniqueProperty;
import be.nabu.libs.types.utils.DateUtils;
import be.nabu.libs.types.utils.DateUtils.Granularity;

public class PostgreSQL implements SQLDialect {

	private Logger logger = LoggerFactory.getLogger(getClass());
	
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
	public String rewrite(String sql, ComplexType input, ComplexType output) {
		Pattern pattern = Pattern.compile("(?<!:)[:$][\\w]+(?!::)(\\b|$|\\Z|\\z)");
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
				else if (Boolean.class.isAssignableFrom(type.getInstanceClass())) {
					postgreType = "boolean";
				}
				if (postgreType != null) {
					result.append("::").append(postgreType);
					boolean isList = element.getType().isList(element.getProperties());
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
		logger.trace("Rewrote '{}'\n{}", new Object[] { sql, result });
		return result.toString();
	}

	@Override
	public String limit(String sql, Integer offset, Integer limit) {
		if (offset != null) {
			sql = sql + " OFFSET " + offset;
		}
		if (limit != null) {
			sql = sql + " LIMIT " + limit;
		}
		return sql;
	}

	@Override
	public String buildCreateSQL(ComplexType type) {
		StringBuilder builder = new StringBuilder();
		builder.append("create table " + EAIRepositoryUtils.uncamelify(getName(type.getProperties())) + " (\n");
		boolean first = true;
		for (Element<?> child : TypeUtils.getAllChildren(type)) {
			if (first) {
				first = false;
			}
			else {
				builder.append(",\n");
			}
			// if we have a complex type, generate an id field that references it
			if (child.getType() instanceof ComplexType) {
				builder.append("\t" + EAIRepositoryUtils.uncamelify(child.getName()) + "_id uuid");
			}
			// differentiate between dates
			else if (Date.class.isAssignableFrom(((SimpleType<?>) child.getType()).getInstanceClass())) {
				Value<String> property = child.getProperty(FormatProperty.getInstance());
				String format = property == null ? "dateTime" : property.getValue();
				if (format.equals("dateTime")) {
					format = "timestamp";
				}
				else if (!format.equals("date") && !format.equals("time")) {
					format = "timestamp";
				}
				builder.append("\t" + EAIRepositoryUtils.uncamelify(child.getName())).append(" ").append(format);
			}
			else {
				builder.append("\t" + EAIRepositoryUtils.uncamelify(child.getName())).append(" ")
					.append(getPredefinedSQLType(((SimpleType<?>) child.getType()).getInstanceClass()));
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
			
			if (child.getName().equals("id")) {
				builder.append(" primary key");
			}
			else {
				Integer value = ValueUtils.getValue(MinOccursProperty.getInstance(), child.getProperties());
				if (value == null || value > 0) {
					builder.append(" not null");
				}
			}
			
			Value<Boolean> property = child.getProperty(UniqueProperty.getInstance());
			if (property != null && property.getValue()) {
				builder.append(" unique");
			}
		}
		builder.append("\n);");
		return builder.toString();
	}

	
	public static String getPredefinedSQLType(Class<?> instanceClass) {
		if (String.class.isAssignableFrom(instanceClass) || char[].class.isAssignableFrom(instanceClass) || URI.class.isAssignableFrom(instanceClass) || instanceClass.isEnum()) {
			// best practice to use application level limits on text
			return "text";
		}
		else if (byte[].class.isAssignableFrom(instanceClass)) {
			return "varbinary";
		}
		else if (Integer.class.isAssignableFrom(instanceClass)) {
			return "integer";
		}
		else if (Long.class.isAssignableFrom(instanceClass)) {
			return "bigint";
		}
		else if (Double.class.isAssignableFrom(instanceClass)) {
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
	public String buildInsertSQL(ComplexContent content) {
		StringBuilder keyBuilder = new StringBuilder();
		StringBuilder valueBuilder = new StringBuilder();
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		formatter.setTimeZone(TimeZone.getTimeZone("UTC"));
		Date date = new Date();
		for (Element<?> element : TypeUtils.getAllChildren(content.getType())) {
			if (element.getType() instanceof SimpleType) {
				Class<?> instanceClass = ((SimpleType<?>) element.getType()).getInstanceClass();
				if (!keyBuilder.toString().isEmpty()) {
					keyBuilder.append(",\n\t");
					valueBuilder.append(",\n\t");
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
						}
						valueBuilder.append(value.toString());
						if (closeQuote) {
							valueBuilder.append("'");
						}
					}
				}
			}
		}
		return "insert into " + EAIRepositoryUtils.uncamelify(getName(content.getType().getProperties())) + " (\n\t" + keyBuilder.toString() + "\n) values (\n\t" + valueBuilder.toString() + "\n);";
	}
}
