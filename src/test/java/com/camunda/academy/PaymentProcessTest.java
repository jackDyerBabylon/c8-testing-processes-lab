package com.camunda.academy;

import com.camunda.academy.handlers.CreditCardChargingHandler;
import com.camunda.academy.handlers.CreditDeductionHandler;
import com.camunda.academy.services.CreditCardService;
import com.camunda.academy.services.CustomerService;
import io.camunda.zeebe.client.ZeebeClient;
import io.camunda.zeebe.client.api.response.ActivateJobsResponse;
import io.camunda.zeebe.client.api.response.ActivatedJob;
import io.camunda.zeebe.client.api.response.DeploymentEvent;
import io.camunda.zeebe.client.api.response.ProcessInstanceEvent;
import io.camunda.zeebe.client.api.worker.JobHandler;
import io.camunda.zeebe.process.test.api.ZeebeTestEngine;
import io.camunda.zeebe.process.test.assertions.BpmnAssert;
import io.camunda.zeebe.process.test.assertions.DeploymentAssert;
import io.camunda.zeebe.process.test.extension.ZeebeProcessTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

import java.io.Serializable;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;

@ZeebeProcessTest
public class PaymentProcessTest {
    private ZeebeTestEngine engine;
    private ZeebeClient client;

    @BeforeEach
    public void setup() {
        DeploymentEvent deploymentEvent = client.newDeployResourceCommand().addResourceFromClasspath("payment.bpmn").send().join();
        DeploymentAssert assertions = BpmnAssert.assertThat(deploymentEvent).containsProcessesByBpmnProcessId("PaymentProcess");

    }

    @Mock
    CustomerService customerServiceMock = mock();

    @Mock
    CreditCardService creditCardServiceMock = mock();

    @Test
    public void testHappyPath() throws Exception {
        // given
        double ORDER_TOTAL = 42.0;
        double CUSTOMER_CREDIT = 50.0;
        Map<String, Object> startVars = Map.of("orderTotal", ORDER_TOTAL, "customerCredit", CUSTOMER_CREDIT);
        JobHandler creditDeductionHandler = new CreditDeductionHandler(customerServiceMock);
        Mockito.when(customerServiceMock.deductCredit(CUSTOMER_CREDIT, ORDER_TOTAL)).thenReturn(0.0);

        // when
        ProcessInstanceEvent processInstance =
                startInstance("PaymentProcess",startVars);
        completeJob("credit-deduction", 1, creditDeductionHandler);

        // then
        Mockito.verify(customerServiceMock).deductCredit(CUSTOMER_CREDIT,ORDER_TOTAL );
        BpmnAssert.assertThat(processInstance)
                .hasVariableWithValue("openAmount", 0.0)
                .hasNotPassedElement("Task_ChargeCreditCard")
                .hasPassedElement("EndEvent_PaymentCompleted")
                .isCompleted();
    }

    @Test
    public void testCreditCardPath() throws Exception {
        // given
        Double OPEN_AMOUNT = 50.0;
        String CARD_NR = "TEST_NR";
        String CVC = "ABC";
        String EXPIRY_DATE = "01/99";
        Map<String, Object> startVars = Map.of(
                "openAmount", OPEN_AMOUNT,
                "expiryDate", EXPIRY_DATE,
                "cardNumber", CARD_NR,
                "cvc", CVC);
        JobHandler creditCardHandler = new CreditCardChargingHandler();

        // when
        ProcessInstanceEvent processInstance =
                startInstanceBefore("PaymentProcess", startVars, "Gateway_CreditSufficient");
        completeUserTask(1, Map.of());
        completeJob("credit-card-charging", 1, creditCardHandler);

        // then
        BpmnAssert.assertThat(processInstance)
                .hasPassedElement("Task_ChargeCreditCard")
                .hasPassedElement("EndEvent_PaymentCompleted")
                .isCompleted();
    }

    public ProcessInstanceEvent startInstance(String id, Map<String, Object> variables){
        ProcessInstanceEvent processInstance = client.newCreateInstanceCommand()
                .bpmnProcessId(id)
                .latestVersion()
                .variables(variables)
                .send().join();
        BpmnAssert.assertThat(processInstance)
                .isStarted();
        return processInstance;
    }

    public ProcessInstanceEvent startInstanceBefore(String id, Map<String, Object> variables, String startingPoint){
        ProcessInstanceEvent processInstance = client.newCreateInstanceCommand()
                .bpmnProcessId(id)
                .latestVersion()
                .variables(variables)
                .startBeforeElement(startingPoint)
                .send().join();
        BpmnAssert.assertThat(processInstance).isStarted();
        return processInstance;
    }

    public void completeUserTask(int count, Map<String, Object> variables) throws Exception {
        ActivateJobsResponse activateJobsResponse = client.newActivateJobsCommand()
                .jobType("io.camunda.zeebe:userTask")
                .maxJobsToActivate(count)
                .send().join();
        List<ActivatedJob> activatedJobs = activateJobsResponse.getJobs();
        if(activatedJobs.size() != count){
            fail("No user task found");
        }

        for (ActivatedJob job:activatedJobs) {
            client.newCompleteCommand(job)
                    .variables(variables)
                    .send().join();
        }
    }

    public void completeJob(String type, int count, JobHandler handler) throws Exception {
        ActivateJobsResponse activateJobsResponse = client.newActivateJobsCommand()
                .jobType(type)
                .maxJobsToActivate(count)
                .send().join();

        List<ActivatedJob> activatedJobs = activateJobsResponse.getJobs();
        if(activatedJobs.size() != count){
            fail("No job activated for type " + type);
        }

        for (ActivatedJob job:activatedJobs) {
            handler.handle(client, job);
        }

        engine.waitForIdleState(Duration.ofSeconds(1));
    }
}

