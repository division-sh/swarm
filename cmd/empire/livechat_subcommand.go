package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	"empireai/internal/config"
	"empireai/internal/events"
	"empireai/internal/runtime"
	runtimeagents "empireai/internal/runtime/agents"
	llm "empireai/internal/runtime/llm"
	runtimemanager "empireai/internal/runtime/manager"
	runtimepipeline "empireai/internal/runtime/pipeline"
	runtimetools "empireai/internal/runtime/tools"
)

func runChatSubcommand(args []string) error {
	fs := flag.NewFlagSet("chat", flag.ContinueOnError)
	cfgPath := fs.String("config", "configs/empire.yaml", "Path to empire config")
	storeMode := fs.String("store", "postgres", "Storage mode")
	migrate := fs.Bool("migrate", false, "Apply migrations")
	migrationFile := fs.String("migration-file", defaultMigrationFilePath, "Migration file path")
	async := fs.Bool("async", false, "Queue messages as events instead of live chat response")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if len(fs.Args()) < 1 {
		return fmt.Errorf("usage: empire chat <vertical/agent|agent-id> [initial message]")
	}

	ctx := context.Background()
	cfg, err := config.Load(*cfgPath)
	if err != nil {
		return err
	}
	stores := buildStores(ctx, *storeMode, cfg, *migrate, *migrationFile)
	target, err := resolveTargetAgent(ctx, stores, fs.Args()[0])
	if err != nil {
		return err
	}
	if err := ensureChatTargetAgentRegistered(ctx, stores, target); err != nil {
		return err
	}

	if *async {
		if len(fs.Args()) > 1 {
			msg := strings.TrimSpace(strings.Join(fs.Args()[1:], " "))
			eventID, err := dispatchBoardMessage(ctx, stores, target, events.EventType("board.chat"), msg)
			if err != nil {
				return err
			}
			fmt.Printf("chat message queued event=%s target=%s\n", eventID, target.ID)
			return nil
		}
		fmt.Printf("chat session target=%s (async queue mode). Type /exit to finish.\n", target.ID)
		scanner := bufio.NewScanner(os.Stdin)
		for {
			fmt.Print("> ")
			if !scanner.Scan() {
				break
			}
			line := strings.TrimSpace(scanner.Text())
			if line == "" {
				continue
			}
			if line == "/exit" || line == "/quit" {
				break
			}
			eventID, err := dispatchBoardMessage(ctx, stores, target, events.EventType("board.chat"), line)
			if err != nil {
				return err
			}
			fmt.Printf("queued event=%s\n", eventID)
		}
		if err := scanner.Err(); err != nil {
			return err
		}
		return nil
	}

	workspaceLifecycle := buildWorkspaceLifecycle(ctx, stores.SQLDB)
	modelRuntime, err := llm.RuntimeFactory{
		Cfg:           cfg,
		Sessions:      stores.SessionRegistry,
		Turns:         stores.TurnStore,
		Conversations: stores.ConversationStore,
		Workspaces:    workspaceLifecycle,
	}.Build()
	if err != nil {
		return err
	}
	bus := runtime.NewEventBus(stores.EventStore)
	var budgetTracker *runtime.BudgetTracker
	if stores.SQLDB != nil {
		budgetTracker = runtime.NewBudgetTracker(stores.SQLDB, bus, cfg, stores.MailboxStore)
	}
	scheduler := runtimepipeline.NewScheduler(func(runtimepipeline.Schedule) {})
	defer scheduler.Stop()
	toolExecutor := runtimetools.NewExecutor(bus, scheduler, nil, stores.ScheduleStore)
	toolExecutor.SetConfig(cfg)
	toolExecutor.SetMailboxStore(stores.MailboxStore)
	toolExecutor.SetSQLDB(stores.SQLDB)
	factory := runtimeagents.NewLLMAgentFactory(modelRuntime, toolExecutor, toolExecutor.ToolDefinitions())
	manager := runtimemanager.NewAgentManager(bus, factory, stores.ManagerStore)
	manager.SetWorkspaceLifecycle(workspaceLifecycle)
	manager.SetSessionRegistry(stores.SessionRegistry, cfg.LLM.RuntimeMode)
	manager.SetBudgetTracker(budgetTracker)
	toolExecutor.SetManager(manager)
	if err := syncRuntimeGlobalAgents(ctx, stores.ManagerStore); err != nil {
		log.Printf("chat command global agents sync failed (continuing): %v", err)
	}
	if err := manager.Recover(ctx); err != nil {
		return fmt.Errorf("recover manager for chat: %w", err)
	}

	if len(fs.Args()) > 1 {
		msg := strings.TrimSpace(strings.Join(fs.Args()[1:], " "))
		if _, err := dispatchBoardMessage(ctx, stores, target, events.EventType("board.chat"), msg); err != nil {
			return err
		}
		resp, err := manager.ChatWithAgent(ctx, target.ID, msg)
		if err != nil {
			return err
		}
		fmt.Println(strings.TrimSpace(resp))
		return nil
	}

	fmt.Printf("chat session target=%s (live). Type /exit to finish.\n", target.ID)
	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("> ")
		if !scanner.Scan() {
			break
		}
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		if line == "/exit" || line == "/quit" {
			break
		}
		if _, err := dispatchBoardMessage(ctx, stores, target, events.EventType("board.chat"), line); err != nil {
			return err
		}
		resp, err := manager.ChatWithAgent(ctx, target.ID, line)
		if err != nil {
			return err
		}
		fmt.Println(strings.TrimSpace(resp))
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	return nil
}
