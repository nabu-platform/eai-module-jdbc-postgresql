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

import java.net.URI;

import be.nabu.eai.module.jdbc.pool.JDBCPoolArtifact;
import be.nabu.eai.module.jdbc.pool.api.JDBCPoolWizard;
import be.nabu.eai.repository.api.Entry;
import be.nabu.eai.repository.resources.RepositoryEntry;
import be.nabu.libs.resources.URIUtils;

public class PostgresWizard implements JDBCPoolWizard<PostgresParameters> {

	@Override
	public String getIcon() {
		return "postgresql-icon.png";
	}

	@Override
	public String getName() {
		return "PostgreSQL";
	}

	@Override
	public Class<PostgresParameters> getWizardClass() {
		return PostgresParameters.class;
	}

	@Override
	public PostgresParameters load(JDBCPoolArtifact pool) {
		String jdbcUrl = pool.getConfig().getJdbcUrl();
		if (jdbcUrl != null && jdbcUrl.startsWith("jdbc:postgresql:")) {
			PostgresParameters parameters = new PostgresParameters();
			try {
				URI uri = new URI(URIUtils.encodeURI(jdbcUrl.substring("jdbc:postgresql:".length())));
				parameters.setHost(uri.getHost());
				parameters.setPort(uri.getPort() < 0 ? null : uri.getPort());
				// replace leading slashes
				parameters.setDatabase(uri.getPath().replaceAll("^[/]+", ""));
				parameters.setUsername(pool.getConfig().getUsername());
				parameters.setPassword(pool.getConfig().getPassword());
				return parameters;
			}
			catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
		return null;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public JDBCPoolArtifact apply(Entry project, RepositoryEntry entry, PostgresParameters properties, boolean isNew, boolean isMain) {
		try {
			JDBCPoolArtifact existing = isNew ? new JDBCPoolArtifact(entry.getId(), entry.getContainer(), entry.getRepository()) : (JDBCPoolArtifact) entry.getNode().getArtifact();
			if (isNew) {
				existing.getConfig().setAutoCommit(false);
			}
			existing.getConfig().setJdbcUrl("jdbc:postgresql://" + (properties.getHost() == null ? "localhost" : properties.getHost()) + ":" + (properties.getPort() == null ? 5432 : properties.getPort()) + "/" + (properties.getDatabase() == null ? "postgres" : properties.getDatabase()));
			Class clazz = PostgreSQL.class;
			existing.getConfig().setDialect(clazz);
			existing.getConfig().setDriverClassName("org.postgresql.Driver");
			existing.getConfig().setUsername(properties.getUsername());
			existing.getConfig().setPassword(properties.getPassword());
			return existing;
		}
		catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

}
