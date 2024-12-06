//
//  Scheduler.cpp
//  CloudSim
//
//  Created by ELMOOTAZBELLAH ELNOZAHY on 10/20/24.
//

#include "Scheduler.hpp"

#include <limits>  // For UINT_MAX
#include <unordered_map>
#include <vector>
static bool migrating = false;
static unsigned active_machines = 16;
static std::unordered_map<VMId_t, std::vector<TaskId_t>> queued_tasks;

void Scheduler::Init()
{
   // Find the parameters of the clusters
   // Get the total number of machines
   // For each machine:
   //      Get the type of the machine
   //      Get the memory of the machine
   //      Get the number of CPUs
   //      Get if there is a GPU or not
   //
   SimOutput("Scheduler::Init(): Total number of machines is " +
                 to_string(Machine_GetTotal()),
             3);
   SimOutput("Scheduler::Init(): Initializing scheduler", 1);
   for(unsigned i = 0; i < active_machines; i++)
   {
      vms.push_back(VM_Create(LINUX, X86));
   }

   for(unsigned i = 0; i < Machine_GetTotal(); i++)
   {
      if(i < active_machines)
      {
         Machine_SetState(MachineId_t(i), S0);  // Turn on initial machines
         machines.push_back(MachineId_t(i));
      }
      else
      {
         Machine_SetState(MachineId_t(i), S5);  // Keep the rest powered down
      }
   }

   for(unsigned i = 0; i < active_machines; i++)
   {
      VM_Attach(vms[i], machines[i]);
   }

   SimOutput("Scheduler::Init(): VM ids are " + to_string(vms[0]) + " ahd " +
                 to_string(vms[1]),
             3);
}

void Scheduler::MigrationComplete(Time_t time, VMId_t vm_id)
{
   // Update your data structure. The VM now can receive new tasks
   migrating = false;
}

void Scheduler::NewTask(Time_t now, TaskId_t task_id)
{
   TaskInfo_t task = GetTaskInfo(task_id);

   // Determine task priority
   Priority_t priority = (task.required_sla == SLA0)   ? HIGH_PRIORITY
                         : (task.required_sla == SLA1) ? MID_PRIORITY
                                                       : LOW_PRIORITY;

   MachineId_t best_machine = std::numeric_limits<MachineId_t>::max();
   VMId_t best_vm = std::numeric_limits<VMId_t>::max();
   unsigned min_tasks =
       std::numeric_limits<unsigned>::max();  // Least loaded machine

   // Find the most compatible machine and VM
   for(MachineId_t i = 0; i < Machine_GetTotal(); ++i)
   {
      MachineInfo_t machine = Machine_GetInfo(i);
      if(machine.s_state == S0 &&
         machine.memory_size - machine.memory_used > task.required_memory &&
         machine.cpu == task.required_cpu)
      {
         unsigned active_tasks_on_machine = 0;

         for(auto& vm_id : vms)
         {
            VMInfo_t vm_info = VM_GetInfo(vm_id);
            if(vm_info.machine_id == i)
            {
               active_tasks_on_machine += vm_info.active_tasks.size();
               if(vm_info.vm_type == task.required_vm &&
                  vm_info.active_tasks.size() < machine.num_cpus && !migrating)
               {
                  best_vm = vm_id;
               }
            }
         }

         if(active_tasks_on_machine < min_tasks)
         {
            best_machine = i;
            min_tasks = active_tasks_on_machine;
         }
      }
   }

   // Assign task to best VM or create new VM
   if(best_vm != std::numeric_limits<VMId_t>::max())
   {
      VM_AddTask(best_vm, task_id, priority);
   }
   else if(best_machine != std::numeric_limits<MachineId_t>::max())
   {
      VMId_t new_vm = VM_Create(task.required_vm, task.required_cpu);
      VM_Attach(new_vm, best_machine);
      VM_AddTask(new_vm, task_id, priority);
      //   printf("Task %u assigned to new VM %u on Machine %u\n", task_id,
      //             new_vm, best_machine);
   }
   else
   {
      //   printf("No suitable machine found for task %u\n", task_id);
   }
}

void Scheduler::PeriodicCheck(Time_t now)
{
   // Count active machines, VMs, and tasks
   unsigned active_machines = 0;
   unsigned active_vms = 0;
   unsigned total_tasks = 0;
   std::vector<MachineId_t> idle_machines;

   for(MachineId_t i = 0; i < Machine_GetTotal(); ++i)
   {
      MachineInfo_t machine = Machine_GetInfo(i);

      if(machine.s_state == S0)
      {
         active_machines++;
         bool has_active_tasks = false;

         // Check VMs on this machine for tasks
         for(auto& vm_id : vms)
         {
            VMInfo_t vm_info = VM_GetInfo(vm_id);

            if(vm_info.machine_id == i)
            {
               total_tasks += vm_info.active_tasks.size();
               if(!vm_info.active_tasks.empty())
               {
                  has_active_tasks = true;
                  active_vms++;
               }
            }
         }

         // If no active tasks on this machine, mark it as idle
         if(!has_active_tasks)
         {
            idle_machines.push_back(i);
         }
      }
   }

   // Task Prioritization Breakdown
   unsigned high_priority_tasks = 0;
   unsigned mid_priority_tasks = 0;
   unsigned low_priority_tasks = 0;

   for(auto& vm_id : vms)
   {
      VMInfo_t vm_info = VM_GetInfo(vm_id);

      for(TaskId_t task_id : vm_info.active_tasks)
      {
         TaskInfo_t task = GetTaskInfo(task_id);

         if(task.priority == HIGH_PRIORITY)
            high_priority_tasks++;
         else if(task.priority == MID_PRIORITY)
            mid_priority_tasks++;
         else
            low_priority_tasks++;
      }
   }

   // Turn off idle machines to save energy
   for(MachineId_t machine_id : idle_machines)
   {
      Machine_SetState(machine_id, S5);  // Power down the machine
      active_machines--;
   }

   // Check if more machines are needed
   if(high_priority_tasks >
      active_vms * 2)  // Example: Each VM handles 2 high-priority tasks
   {
      for(MachineId_t i = 0; i < Machine_GetTotal(); ++i)
      {
         MachineInfo_t machine = Machine_GetInfo(i);

         if(machine.s_state == S5)  // Find a powered-down machine
         {
            Machine_SetState(i, S0);  // Power it on

            // Create a new VM on this machine
            VMId_t new_vm = VM_Create(LINUX, machine.cpu);
            VM_Attach(new_vm, i);
            vms.push_back(new_vm);

            active_machines++;
            active_vms++;
            break;  // Power on one machine at a time
         }
      }
   }

   // Rebalance tasks if necessary
   if(active_machines > total_tasks)
   {
      for(auto& vm_id : vms)
      {
         VMInfo_t vm_info = VM_GetInfo(vm_id);

         if(vm_info.active_tasks.empty() &&
            vm_info.machine_id < active_machines)
         {
            // Migrate this VM to a busier machine
            for(MachineId_t i = 0; i < Machine_GetTotal(); ++i)
            {
               if(i != vm_info.machine_id)
               {
                  MachineInfo_t target_machine = Machine_GetInfo(i);

                  if(target_machine.s_state == S0 &&
                     target_machine.memory_size - target_machine.memory_used >
                         1024)
                  {
                     VM_Migrate(vm_id, i);
                     break;
                  }
               }
            }
         }
      }
   }
}

void Scheduler::Shutdown(Time_t time)
{
   // Do your final reporting and bookkeeping here.
   // Report about the total energy consumed
   // Report about the SLA compliance
   // Shutdown everything to be tidy :-)
   for(auto& vm : vms)
   {
      VM_Shutdown(vm);
   }
   SimOutput("SimulationComplete(): Finished!", 4);
   SimOutput("SimulationComplete(): Time is " + to_string(time), 4);
}

void Scheduler::TaskComplete(Time_t now, TaskId_t task_id)
{
   // Iterate over all VMs to find the task
   for(auto& vm_id : vms)
   {
      VMInfo_t vm_info = VM_GetInfo(vm_id);

      // Manual search for the task in the active tasks of the VM
      auto it = vm_info.active_tasks.begin();
      for(; it != vm_info.active_tasks.end(); ++it)
      {
         if(*it == task_id)
         {
            // Task found in this VM, remove it
            vm_info.active_tasks.erase(it);

            // Decide if the machine needs adjustments
            MachineId_t machine_id = vm_info.machine_id;
            MachineInfo_t machine_info = Machine_GetInfo(machine_id);

            // Check if memory is low or underutilized
            if(machine_info.memory_size - machine_info.memory_used < 1024)
            {
               // Migrate tasks to another machine with more available memory
               for(auto& target_machine_id : machines)
               {
                  if(target_machine_id != machine_id)
                  {
                     MachineInfo_t target_machine_info =
                         Machine_GetInfo(target_machine_id);
                     if(target_machine_info.s_state == S0 &&
                        target_machine_info.memory_size -
                                target_machine_info.memory_used >
                            machine_info.memory_used)
                     {
                        VM_Migrate(vm_id, target_machine_id);
                        break;
                     }
                  }
               }
            }

            // Break after processing the task
            return;
         }
      }
   }
}

// Public interface below

static Scheduler Scheduler;

void InitScheduler()
{
   SimOutput("InitScheduler(): Initializing scheduler", 4);
   Scheduler.Init();
}

void HandleNewTask(Time_t time, TaskId_t task_id)
{
   SimOutput("HandleNewTask(): Received new task " + to_string(task_id) +
                 " at time " + to_string(time),
             4);
   Scheduler.NewTask(time, task_id);
}

void HandleTaskCompletion(Time_t time, TaskId_t task_id)
{
   SimOutput("HandleTaskCompletion(): Task " + to_string(task_id) +
                 " completed at time " + to_string(time),
             4);
   Scheduler.TaskComplete(time, task_id);
}

void MemoryWarning(Time_t time, MachineId_t machine_id)
{
   // The simulator is alerting you that machine identified by machine_id is
   // overcommitted
   SimOutput("MemoryWarning(): Overflow at " + to_string(machine_id) +
                 " was detected at time " + to_string(time),
             0);
}

void MigrationDone(Time_t time, VMId_t vm_id)
{
   // The function is called on to alert you that migration is complete
   SimOutput("MigrationDone(): Migration of VM " + to_string(vm_id) +
                 " was completed at time " + to_string(time),
             4);
   Scheduler.MigrationComplete(time, vm_id);
   migrating = false;
}

void SchedulerCheck(Time_t time)
{
   // This function is called periodically by the simulator, no specific
   // event
   SimOutput("SchedulerCheck(): SchedulerCheck() called at " + to_string(time),
             4);
   Scheduler.PeriodicCheck(time);
   static unsigned counts = 0;
   counts++;
   if(counts == 10)
   {
      migrating = true;
      VM_Migrate(1, 9);
   }
}

void SimulationComplete(Time_t time)
{
   // This function is called before the simulation terminates Add whatever
   // you feel like.
   cout << "SLA violation report" << endl;
   cout << "SLA0: " << GetSLAReport(SLA0) << "%" << endl;
   cout << "SLA1: " << GetSLAReport(SLA1) << "%" << endl;
   cout << "SLA2: " << GetSLAReport(SLA2) << "%"
        << endl;  // SLA3 do not have SLA violation issues
   cout << "Total Energy " << Machine_GetClusterEnergy() << "KW-Hour" << endl;
   cout << "Simulation run finished in " << double(time) / 1000000 << " seconds"
        << endl;
   SimOutput(
       "SimulationComplete(): Simulation finished at time " + to_string(time),
       4);

   Scheduler.Shutdown(time);
}

void SLAWarning(Time_t time, TaskId_t task_id) {}

void StateChangeComplete(Time_t time, MachineId_t machine_id)
{
   // Called in response to an earlier request to change the state of a
   // machine
}
