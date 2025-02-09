
package org.apache.fineract.portfolio.savings.data;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import org.apache.fineract.infrastructure.core.domain.JdbcSupport;
import org.apache.fineract.infrastructure.core.service.DateUtils;
import org.apache.fineract.organisation.monetary.data.CurrencyData;
import org.springframework.jdbc.core.RowMapper;

public class SavingsAccountTransactionTemplateMapperFc implements RowMapper<SavingsAccountTransactionDataFc> {

    private final String schemaSql;

    public SavingsAccountTransactionTemplateMapperFc() {
        final StringBuilder sqlBuilder = new StringBuilder(400);
        sqlBuilder.append("sa.id as id, sa.account_no as accountNo, ");
        sqlBuilder.append(
                "sa.currency_code as currencyCode, sa.currency_digits as currencyDigits, sa.currency_multiplesof as inMultiplesOf, ");
        sqlBuilder.append("curr.name as currencyName, curr.internationalized_name_code as currencyNameCode, ");
        sqlBuilder.append("curr.display_symbol as currencyDisplaySymbol, ");
        sqlBuilder.append("sa.min_required_opening_balance as minRequiredOpeningBalance ");
        sqlBuilder.append("from m_savings_account sa ");
        sqlBuilder.append("join m_currency curr on curr.code = sa.currency_code ");

        this.schemaSql = sqlBuilder.toString();
    }

    public String schema() {
        return this.schemaSql;
    }
    
    ZonedDateTime convertToZonedDateTimeUsingInstant(Timestamp timestamp) {
        Instant instant = timestamp.toInstant();
        return instant.atZone(ZoneId.systemDefault());
    }

    @Override
    public SavingsAccountTransactionDataFc mapRow(final ResultSet rs, @SuppressWarnings("unused") final int rowNum) throws SQLException {

        final Long savingsId = rs.getLong("id");
        final String accountNo = rs.getString("accountNo");
        final ZonedDateTime createdDate = convertToZonedDateTimeUsingInstant(rs.getTimestamp("createdDate"));
        final String currencyCode = rs.getString("currencyCode");
        final String currencyName = rs.getString("currencyName");
        final String currencyNameCode = rs.getString("currencyNameCode");
        final String currencyDisplaySymbol = rs.getString("currencyDisplaySymbol");
        final Integer currencyDigits = JdbcSupport.getInteger(rs, "currencyDigits");
        final Integer inMultiplesOf = JdbcSupport.getInteger(rs, "inMultiplesOf");
        final CurrencyData currency = new CurrencyData(currencyCode, currencyName, currencyDigits, inMultiplesOf, currencyDisplaySymbol,
                currencyNameCode);

        return SavingsAccountTransactionDataFc.template(savingsId, accountNo, DateUtils.getBusinessLocalDate(),createdDate, currency);
    }
}