package com.camunda.academy;

import io.camunda.zeebe.client.ZeebeClient;
import io.camunda.zeebe.client.api.response.ActivateJobsResponse;
import io.camunda.zeebe.client.api.response.ActivatedJob;
import io.camunda.zeebe.client.api.response.DeploymentEvent;
import io.camunda.zeebe.client.api.response.ProcessInstanceEvent;
import io.camunda.zeebe.process.test.api.ZeebeTestEngine;
import io.camunda.zeebe.process.test.extension.ZeebeProcessTest;
import io.camunda.zeebe.process.test.filters.RecordStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static io.camunda.zeebe.process.test.assertions.BpmnAssert.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

@ZeebeProcessTest
public class HardwareRequestProcessTest {

    private ZeebeTestEngine engine;
    private ZeebeClient client;
    private RecordStream recordStream;

    @BeforeEach
    public void setup(){
        DeploymentEvent deploymentEvent = client.newDeployResourceCommand()
                .addResourceFromClasspath("hardwarerequest.bpmn")
                .send()
                .join();
        assertThat(deploymentEvent).containsProcessesByBpmnProcessId("HardwareRequestProcess");
    }

    @Test
    public void testDeployment(){

    }

    @Test
    public void testHappyPath() throws Exception {
        // given
        double PRICE = 240;
        Map startVars = Map.of("price", PRICE);

        // when
        ProcessInstanceEvent processInstance = startInstance("HardwareRequestProcess", startVars);
        completeJob("check-availability", 1, Map.of("available", true));
        completeJob("send-hardware", 1, Map.of());

        // then
        assertThat(processInstance).hasPassedElement("EndEvent_HardwareSent")
                .isCompleted();
    }
    @Test
    public void testHardwareNotAvailable() throws Exception {
        // given
        boolean AVAILABLE = false;
        String ORDER_ID = "test";
        Map startVars = Map.of("available", AVAILABLE, "orderId", ORDER_ID);

        // when
        ProcessInstanceEvent processInstance = startInstanceBefore("HardwareRequestProcess", startVars, "Gateway_HardwareAvailable");
        completeJob("order-hardware", 1, Map.of());
        client.newPublishMessageCommand().messageName("hardwareReceived")
                .correlationKey("test")
                .send().join();

        // then
        assertThat(processInstance).isWaitingAtElements("ServiceTask_SendHardware");

    }

    @Test
    public void testSupplierDelay() throws Exception {
        // given
        String ORDER_ID = "test";
        Map startVars = Map.of("orderId", ORDER_ID);

        // when
        ProcessInstanceEvent processInstance = startInstanceBefore("HardwareRequestProcess", startVars,"Gateway_WaitForHardware");
        engine.increaseTime(Duration.ofDays(7));
        engine.waitForBusyState(Duration.ofSeconds(1));
        engine.waitForIdleState(Duration.ofSeconds(1));
        completeUserTask(1, Map.of());

        // then
        assertThat(processInstance).isWaitingAtElements("Gateway_WaitForHardware");

    }

    @Test
    public void testApproval() throws Exception {
        // given
        double PRICE = 1230;
        List<String> approvers = new LinkedList<>();
        approvers.add("john");
        approvers.add("lisa");
        approvers.add("max");
        Map<String, Object> startVars = Map.of("price", PRICE, "approvers", approvers);

        // when
        ProcessInstanceEvent processInstance = startInstance("HardwareRequestProcess", startVars);
        completeUserTask(3, Map.of("approved", true));

        //then
        assertThat(processInstance).isWaitingAtElements("ServiceTask_CheckAvailability");
    }

    @Test
    public void testRejection() throws Exception {
        // given
        double PRICE = 1230;
        List<String> approvers = new LinkedList<>();
        approvers.add("john");
        approvers.add("lisa");
        approvers.add("max");
        Map<String, Object> startVars = Map.of("price", PRICE, "approvers", approvers);

        // when
        ProcessInstanceEvent processInstance = startInstance("HardwareRequestProcess", startVars);
        completeUserTask(2, Map.of("approved", true));
        completeUserTask(1, Map.of("approved", false));

        //then
        assertThat(processInstance).hasPassedElement("EndEvent_OrderRejected")
                .isCompleted();
    }

    @Test
    public void testErrorPath() throws Exception {
        // given

        // when
        ProcessInstanceEvent processInstance =
                startInstanceBefore("HardwareRequestProcess", Map.of(), "ServiceTask_SendHardware");
        completeJobWithError("send-hardware", 1, "stolen");
        completeJob("inform-requester", 1, Map.of());

        // then
        assertThat(processInstance).hasPassedElement("EndEvent_HardwareStolen")
                .isCompleted();
    }

    public void completeJob(String type, int count, Map<String, Object> variables) throws Exception {
        ActivateJobsResponse activateJobsResponse = client.newActivateJobsCommand()
                .jobType(type)
                .maxJobsToActivate(count)
                .send().join();
        List<ActivatedJob> activatedJobs = activateJobsResponse.getJobs();
        if(activatedJobs.size() != count){
            fail("No task found for " + type);
        }

        for (ActivatedJob job:activatedJobs) {
            client.newCompleteCommand(job)
                    .variables(variables)
                    .send().join();
        }
    }

    public void completeJobWithError(String type, int count, String errorCode) throws Exception {
        ActivateJobsResponse activateJobsResponse = client.newActivateJobsCommand()
                .jobType(type)
                .maxJobsToActivate(count)
                .send().join();
        List<ActivatedJob> activatedJobs = activateJobsResponse.getJobs();
        if(activatedJobs.size() != count){
            fail("No task found for " + type);
        }

        for (ActivatedJob job:activatedJobs) {
            client.newThrowErrorCommand(job)
                    .errorCode(errorCode)
                    .send().join();
        }
    }

    public void completeUserTask(int count, Map<String, Object> variables) throws Exception {
        completeJob("io.camunda.zeebe:userTask", count, variables);
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
