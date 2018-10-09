package be.nabu.eai.module.jdbc.dialects;

import org.postgresql.util.PGInterval;

import be.nabu.libs.converter.api.ConverterProvider;
import be.nabu.libs.types.base.Duration;

public class PGIntervalToString implements ConverterProvider<PGInterval, String> {

	@Override
	public String convert(PGInterval instance) {
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
		return duration.toString();
	}

	@Override
	public Class<PGInterval> getSourceClass() {
		return PGInterval.class;
	}

	@Override
	public Class<String> getTargetClass() {
		return String.class;
	}

}
