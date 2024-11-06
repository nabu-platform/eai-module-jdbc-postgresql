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

import org.postgresql.util.PGInterval;

import be.nabu.libs.converter.api.ConverterProvider;
import be.nabu.libs.types.base.Duration;

public class PGIntervalToDuration implements ConverterProvider<PGInterval, Duration> {

	@Override
	public Duration convert(PGInterval instance) {
		if (instance == null) {
			return null;
		}
		Duration duration = new Duration();
		duration.setSeconds(instance.getSeconds());
		duration.setMinutes(instance.getMinutes());
		duration.setHours(instance.getHours());
		duration.setDays(instance.getDays());
		duration.setMonths(instance.getMonths());
		duration.setYears(instance.getYears());
		return duration;
	}

	@Override
	public Class<PGInterval> getSourceClass() {
		return PGInterval.class;
	}

	@Override
	public Class<Duration> getTargetClass() {
		return Duration.class;
	}

}
