/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.fineract.portfolio.savings.starter;

import org.apache.fineract.infrastructure.core.service.PaginationHelper;
import org.apache.fineract.infrastructure.core.service.database.DatabaseSpecificSQLGenerator;
import org.apache.fineract.infrastructure.dataqueries.service.EntityDatatableChecksReadService;
import org.apache.fineract.infrastructure.security.service.PlatformSecurityContext;
import org.apache.fineract.infrastructure.security.utils.ColumnValidator;
import org.apache.fineract.organisation.staff.service.StaffReadPlatformService;
import org.apache.fineract.portfolio.charge.service.ChargeReadPlatformService;
import org.apache.fineract.portfolio.client.service.ClientReadPlatformService;
import org.apache.fineract.portfolio.group.service.GroupReadPlatformService;
import org.apache.fineract.portfolio.savings.domain.SavingsAccountAssembler;
import org.apache.fineract.portfolio.savings.domain.SavingsAccountRepositoryWrapper;
import org.apache.fineract.portfolio.savings.service.SavingsAccountReadPlatformService;
import org.apache.fineract.portfolio.savings.service.SavingsAccountReadPlatformServiceImplFc;
import org.apache.fineract.portfolio.savings.service.SavingsDropdownReadPlatformService;
import org.apache.fineract.portfolio.savings.service.SavingsProductReadPlatformService;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;

@Configuration
public class SavingsConfigurationFc {
    
    @Bean
    @ConditionalOnMissingBean(SavingsAccountReadPlatformService.class)
    public SavingsAccountReadPlatformServiceImplFc savingsAccountReadPlatformServiceFc(PlatformSecurityContext context, JdbcTemplate jdbcTemplate,
            ClientReadPlatformService clientReadPlatformService, GroupReadPlatformService groupReadPlatformService,
            SavingsProductReadPlatformService savingProductReadPlatformService, StaffReadPlatformService staffReadPlatformService,
            SavingsDropdownReadPlatformService dropdownReadPlatformService, ChargeReadPlatformService chargeReadPlatformService,
            EntityDatatableChecksReadService entityDatatableChecksReadService, ColumnValidator columnValidator,
            SavingsAccountAssembler savingAccountAssembler, PaginationHelper paginationHelper, DatabaseSpecificSQLGenerator sqlGenerator,
            SavingsAccountRepositoryWrapper savingsAccountRepositoryWrapper) {
        return new SavingsAccountReadPlatformServiceImplFc(context, jdbcTemplate, clientReadPlatformService, groupReadPlatformService,
                savingProductReadPlatformService, staffReadPlatformService, dropdownReadPlatformService, chargeReadPlatformService,
                entityDatatableChecksReadService, columnValidator, savingAccountAssembler, paginationHelper, sqlGenerator,
                savingsAccountRepositoryWrapper);
    }
    
}
