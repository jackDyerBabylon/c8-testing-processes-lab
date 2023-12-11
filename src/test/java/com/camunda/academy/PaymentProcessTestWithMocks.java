package com.camunda.academy;

import com.camunda.academy.services.CreditCardService;
import com.camunda.academy.services.CustomerService;
import com.camunda.academy.handler.CreditCardChargingHandler;
import com.camunda.academy.handler.CreditDeductionHandler;
import io.camunda.zeebe.client.ZeebeClient;
import io.camunda.zeebe.client.api.response.ActivateJobsResponse;
import io.camunda.zeebe.client.api.response.ActivatedJob;
import io.camunda.zeebe.client.api.response.DeploymentEvent;
import io.camunda.zeebe.client.api.response.ProcessInstanceEvent;
import io.camunda.zeebe.client.api.worker.JobHandler;
import io.camunda.zeebe.process.test.api.ZeebeTestEngine;
import io.camunda.zeebe.process.test.extension.ZeebeProcessTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static io.camunda.zeebe.process.test.assertions.BpmnAssert.assertThat;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

@ZeebeProcessTest
public class PaymentProcessTestWithMocks {

    private ZeebeTestEngine engine;
    private ZeebeClient client;

    @Mock
    CustomerService customerServiceMock = mock();

    @Mock
    CreditCardService creditCardServiceMock = mock();

    @BeforeEach
    public void setup(){
        DeploymentEvent deploymentEvent = client.newDeployResourceCommand()
                .addResourceFromClasspath("payment.bpmn")
                .send()
                .join();
        assertThat(deploymentEvent).containsProcessesByBpmnProcessId("PaymentProcess");
    }

    @Test
    public void testDeployment(){

    }

    @Test
    public void testHappyPath() throws Exception {
        // given
        double ORDER_TOTAL = 42.0;
        double CUSTOMER_CREDIT = 50.0;
        Map startVars = Map.of("orderTotal", ORDER_TOTAL, "customerCredit", CUSTOMER_CREDIT);
        JobHandler creditDeductionHandler = new CreditDeductionHandler(customerServiceMock);
        Mockito.when(customerServiceMock.deductCredit(CUSTOMER_CREDIT, ORDER_TOTAL)).thenReturn(0.0);

        // when
        ProcessInstanceEvent processInstance =
                startInstance("PaymentProcess",startVars);
        completeJob("credit-deduction", 1, creditDeductionHandler);

        // then
        Mockito.verify(customerServiceMock).deductCredit(CUSTOMER_CREDIT,ORDER_TOTAL );
        assertThat(processInstance)
                .hasVariableWithValue("openAmount", 0.0)
                .hasPassedElement("EndEvent_PaymentCompleted")
                .hasNotPassedElement("Task_ChargeCreditCard")
                .isCompleted();
    }

    @Test
    public void testCreditCardPath() throws Exception {
        // given
        double OPEN_AMOUNT = 50.0;
        String CARD_NR = "TEST_NR";
        String CVC = "ABC";
        String EXPIRY_DATE = "01/99";
        Map startVars = Map.of(
                "openAmount", OPEN_AMOUNT,
                "expiryDate", EXPIRY_DATE,
                "cardNumber", CARD_NR,
                "cvc", CVC);
        JobHandler creditCardHandler = new CreditCardChargingHandler(creditCardServiceMock);

        // when
        ProcessInstanceEvent processInstance =
                startInstanceBefore("PaymentProcess", startVars, "Gateway_CreditSufficient");
        completeUserTask(1, Map.of());
        completeJob("credit-card-charging", 1, creditCardHandler);

        // then
        Mockito.verify(creditCardServiceMock).chargeAmount(CARD_NR, CVC, EXPIRY_DATE, 50.0);
        assertThat(processInstance).hasPassedElement("Task_ChargeCreditCard")
                .isCompleted();

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

    public ProcessInstanceEvent startInstance(String id, Map<String, Object> variables){
        ProcessInstanceEvent processInstance = client.newCreateInstanceCommand()
                .bpmnProcessId(id)
                .latestVersion()
                .variables(variables)
                .send().join();
        assertThat(processInstance).isStarted();
        return processInstance;
    }

    public ProcessInstanceEvent startInstanceBefore(String id, Map<String, Object> variables, String startingPoint){
        ProcessInstanceEvent processInstance = client.newCreateInstanceCommand()
                .bpmnProcessId(id)
                .latestVersion()
                .variables(variables)
                .startBeforeElement(startingPoint)
                .send().join();
        assertThat(processInstance).isStarted();
        return processInstance;
    }


}
