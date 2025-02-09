/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package org.apache.fineract.portfolio.savings.data;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.ZonedDateTime;
import static org.apache.fineract.infrastructure.core.domain.AuditableFieldsConstants.CREATED_BY_DB_FIELD;
import org.apache.fineract.infrastructure.core.domain.JdbcSupport;
import org.apache.fineract.organisation.monetary.data.CurrencyData;
import org.apache.fineract.portfolio.account.data.AccountTransferData;
import org.apache.fineract.portfolio.paymentdetail.data.PaymentDetailData;
import org.apache.fineract.portfolio.paymenttype.data.PaymentTypeData;
import org.apache.fineract.portfolio.savings.service.SavingsEnumerations;
import org.springframework.jdbc.core.RowMapper;

public class SavingsAccountTransactionsMapperFc implements RowMapper<SavingsAccountTransactionDataFc> {

    public static final String TRANSACTION_ID = "transactionId";
    public static final String TRANSACTION_TYPE = "transactionType";
    public static final String TRANSACTION_DATE = "transactionDate";
    public static final String CREATED_ON_UTC = "createdDate";
    public static final String TRANSACTION_AMOUNT = "transactionAmount";
    public static final String RUNNING_BALANCE = "runningBalance";
    public static final String REVERSED = "reversed";
    public static final String SAVINGS_ID = "savingsId";
    public static final String ACCOUNT_NO = "accountNo";
    public static final String PAYMENT_TYPE = "paymentType";
    public static final String PAYMENT_TYPE_NAME = "paymentTypeName";
    public static final String ACCOUNT_NUMBER = "accountNumber";
    public static final String CHECK_NUMBER = "checkNumber";
    public static final String ROUTING_CODE = "routingCode";
    public static final String RECEIPT_NUMBER = "receiptNumber";
    public static final String BANK_NUMBER = "bankNumber";
    public static final String CURRENCY_CODE = "currencyCode";
    public static final String CURRENCY_NAME = "currencyName";
    public static final String CURRENCY_NAME_CODE = "currencyNameCode";
    public static final String CURRENCY_DISPLAY_SYMBOL = "currencyDisplaySymbol";
    public static final String CURRENCY_DIGITS = "currencyDigits";
    public static final String IN_MULTIPLES_OF = "inMultiplesOf";
    public static final String FROM_TRANSFER_ID = "fromTransferId";
    public static final String TO_TRANSFER_ID = "toTransferId";
    public static final String FROM_TRANSFER_DATE = "fromTransferDate";
    public static final String FROM_TRANSFER_AMOUNT = "fromTransferAmount";
    public static final String FROM_TRANSFER_REVERSED = "fromTransferReversed";
    public static final String FROM_TRANSFER_DESCRIPTION = "fromTransferDescription";
    public static final String TO_TRANSFER_DATE = "toTransferDate";
    public static final String TO_TRANSFER_AMOUNT = "toTransferAmount";
    public static final String TO_TRANSFER_REVERSED = "toTransferReversed";
    public static final String TO_TRANSFER_DESCRIPTION = "toTransferDescription";
    public static final String SUBMITTED_BY_USERNAME = "submittedByUsername";
    public static final String SUBMITTED_ON_DATE = "submittedOnDate";
    private final String schemaSql;

    public SavingsAccountTransactionsMapperFc() {

        final StringBuilder sqlBuilder = new StringBuilder(400);
        sqlBuilder.append("tr.id as transactionId, tr.transaction_type_enum as transactionType, ");
        sqlBuilder.append("tr.transaction_date as transactionDate, tr.amount as transactionAmount, ");
        sqlBuilder.append("tr.created_on_utc as createdDate, ");
        sqlBuilder.append("tr.running_balance_derived as runningBalance, tr.is_reversed as reversed,");
        sqlBuilder.append("tr.submitted_on_date as submittedOnDate,");
        sqlBuilder.append("fromtran.id as fromTransferId, fromtran.is_reversed as fromTransferReversed,");
        sqlBuilder.append("fromtran.transaction_date as fromTransferDate, fromtran.amount as fromTransferAmount,");
        sqlBuilder.append("fromtran.description as fromTransferDescription,");
        sqlBuilder.append("totran.id as toTransferId, totran.is_reversed as toTransferReversed,");
        sqlBuilder.append("totran.transaction_date as toTransferDate, totran.amount as toTransferAmount,");
        sqlBuilder.append("totran.description as toTransferDescription,");
        sqlBuilder.append("sa.id as savingsId, sa.account_no as accountNo,");
        sqlBuilder.append(" au.username as submittedByUsername, ");
        sqlBuilder.append("pd.payment_type_id as paymentType,pd.account_number as accountNumber,pd.check_number as checkNumber, ");
        sqlBuilder.append("pd.receipt_number as receiptNumber, pd.bank_number as bankNumber,pd.routing_code as routingCode, ");
        sqlBuilder.append(
                "sa.currency_code as currencyCode, sa.currency_digits as currencyDigits, sa.currency_multiplesof as inMultiplesOf, ");
        sqlBuilder.append("curr.name as currencyName, curr.internationalized_name_code as currencyNameCode, ");
        sqlBuilder.append("curr.display_symbol as currencyDisplaySymbol, ");
        sqlBuilder.append("pt.value as paymentTypeName ");
        sqlBuilder.append("from m_savings_account sa ");
        sqlBuilder.append("join m_savings_account_transaction tr on tr.savings_account_id = sa.id ");
        sqlBuilder.append("join m_currency curr on curr.code = sa.currency_code ");
        sqlBuilder.append("left join m_account_transfer_transaction fromtran on fromtran.from_savings_transaction_id = tr.id ");
        sqlBuilder.append("left join m_account_transfer_transaction totran on totran.to_savings_transaction_id = tr.id ");
        sqlBuilder.append("left join m_payment_detail pd on tr.payment_detail_id = pd.id ");
        sqlBuilder.append("left join m_payment_type pt on pd.payment_type_id = pt.id ");
        sqlBuilder.append("left join m_appuser au on au.id = tr." + CREATED_BY_DB_FIELD);
        this.schemaSql = sqlBuilder.toString();
    }

    public String schema() {
        return this.schemaSql;
    }

    @Override
    public SavingsAccountTransactionDataFc mapRow(final ResultSet rs, @SuppressWarnings("unused") final int rowNum) throws SQLException {
        final Long id = rs.getLong(TRANSACTION_ID);
        final int transactionTypeInt = JdbcSupport.getInteger(rs, TRANSACTION_TYPE);
        final SavingsAccountTransactionEnumData transactionType = SavingsEnumerations.transactionType(transactionTypeInt);

        final LocalDate date = JdbcSupport.getLocalDate(rs, TRANSACTION_DATE);
        final LocalDate submittedOnDate = JdbcSupport.getLocalDate(rs, SUBMITTED_ON_DATE);
        final ZonedDateTime createdDate = JdbcSupport.getDateTime(rs, CREATED_ON_UTC);
        final BigDecimal amount = JdbcSupport.getBigDecimalDefaultToZeroIfNull(rs, TRANSACTION_AMOUNT);
        final BigDecimal outstandingChargeAmount = null;
        final BigDecimal runningBalance = JdbcSupport.getBigDecimalDefaultToZeroIfNull(rs, RUNNING_BALANCE);
        final boolean reversed = rs.getBoolean(REVERSED);

        final Long savingsId = rs.getLong(SAVINGS_ID);
        final String accountNo = rs.getString(ACCOUNT_NO);

        PaymentDetailData paymentDetailData = null;
        if (transactionType.isDepositOrWithdrawal()) {
            final Long paymentTypeId = JdbcSupport.getLong(rs, PAYMENT_TYPE);
            if (paymentTypeId != null) {
                final String typeName = rs.getString(PAYMENT_TYPE_NAME);
                final PaymentTypeData paymentType = PaymentTypeData.instance(paymentTypeId, typeName);
                final String accountNumber = rs.getString(ACCOUNT_NUMBER);
                final String checkNumber = rs.getString(CHECK_NUMBER);
                final String routingCode = rs.getString(ROUTING_CODE);
                final String receiptNumber = rs.getString(RECEIPT_NUMBER);
                final String bankNumber = rs.getString(BANK_NUMBER);
                paymentDetailData = new PaymentDetailData(id, paymentType, accountNumber, checkNumber, routingCode, receiptNumber,
                        bankNumber);
            }
        }

        final String currencyCode = rs.getString(CURRENCY_CODE);
        final String currencyName = rs.getString(CURRENCY_NAME);
        final String currencyNameCode = rs.getString(CURRENCY_NAME_CODE);
        final String currencyDisplaySymbol = rs.getString(CURRENCY_DISPLAY_SYMBOL);
        final Integer currencyDigits = JdbcSupport.getInteger(rs, CURRENCY_DIGITS);
        final Integer inMultiplesOf = JdbcSupport.getInteger(rs, IN_MULTIPLES_OF);
        final CurrencyData currency = new CurrencyData(currencyCode, currencyName, currencyDigits, inMultiplesOf, currencyDisplaySymbol,
                currencyNameCode);

        AccountTransferData transfer = null;
        final Long fromTransferId = JdbcSupport.getLong(rs, FROM_TRANSFER_ID);
        final Long toTransferId = JdbcSupport.getLong(rs, TO_TRANSFER_ID);
        if (fromTransferId != null) {
            final LocalDate fromTransferDate = JdbcSupport.getLocalDate(rs, FROM_TRANSFER_DATE);
            final BigDecimal fromTransferAmount = JdbcSupport.getBigDecimalDefaultToZeroIfNull(rs, FROM_TRANSFER_AMOUNT);
            final boolean fromTransferReversed = rs.getBoolean(FROM_TRANSFER_REVERSED);
            final String fromTransferDescription = rs.getString(FROM_TRANSFER_DESCRIPTION);

            transfer = AccountTransferData.transferBasicDetails(fromTransferId, currency, fromTransferAmount, fromTransferDate,
                    fromTransferDescription, fromTransferReversed);
        } else if (toTransferId != null) {
            final LocalDate toTransferDate = JdbcSupport.getLocalDate(rs, TO_TRANSFER_DATE);
            final BigDecimal toTransferAmount = JdbcSupport.getBigDecimalDefaultToZeroIfNull(rs, TO_TRANSFER_AMOUNT);
            final boolean toTransferReversed = rs.getBoolean(TO_TRANSFER_REVERSED);
            final String toTransferDescription = rs.getString(TO_TRANSFER_DESCRIPTION);

            transfer = AccountTransferData.transferBasicDetails(toTransferId, currency, toTransferAmount, toTransferDate,
                    toTransferDescription, toTransferReversed);
        }
        final boolean postInterestAsOn = false;
        final String submittedByUsername = rs.getString(SUBMITTED_BY_USERNAME);
        final String note = null;
        return SavingsAccountTransactionDataFc.create(id, transactionType, paymentDetailData, savingsId, accountNo, date, currency,
                amount, outstandingChargeAmount, runningBalance, reversed, transfer, postInterestAsOn, submittedByUsername, note,
                submittedOnDate, createdDate);
    }
}