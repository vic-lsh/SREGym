import asyncio
import logging
from collections import deque

from langchain_core.messages import HumanMessage, SystemMessage, ToolMessage
from langgraph.types import Command

from llm_backend.token_util import count_tokens, get_context_window

logger = logging.getLogger(__name__)


def _build_tool_map(tools: list) -> tuple[dict, dict]:
    """Split tools into {name: tool} dicts for sync and async tools."""
    sync_tools, async_tools = {}, {}
    for t in tools:
        if getattr(t, "coroutine", None) is not None:
            async_tools[t.name] = t
        else:
            sync_tools[t.name] = t
    return sync_tools, async_tools


class CrucibleAgent:
    """ReAct agent as a plain async loop — no StateGraph required.

    Each iteration:
      1. Call LLM with all tools
      2. If the response has tool calls, execute them in parallel and collect state updates
      3. After each round of tool results, compact context if approaching the window limit
      4. Stop when submitted=True or the LLM produces no tool calls
    """

    def __init__(self, llm, tools: list, submit_tool, model_name: str, role: str = "agent"):
        self.llm = llm
        self.tools = tools
        self.submit_tool = submit_tool
        self.model_name = model_name
        self.role = role
        self.context_window = get_context_window(model_name)
        self._sync_tools, self._async_tools = _build_tool_map(tools)

    async def _invoke_tool(self, tool_call: dict) -> tuple[list, dict]:
        """Invoke a single tool call. Returns (tool_messages, state_updates)."""
        name = tool_call["name"]
        tool_input = {"type": "tool_call", "name": name, "args": tool_call["args"], "id": tool_call["id"]}

        if name in self._async_tools:
            result = await self._async_tools[name].ainvoke(tool_input)
        elif name in self._sync_tools:
            result = self._sync_tools[name].invoke(tool_input)
        else:
            logger.warning(f"[{self.role}] Tool '{name}' not found.")
            return [ToolMessage(content=f"Tool '{name}' not found.", tool_call_id=tool_call["id"])], {}

        if not isinstance(result, Command):
            logger.error(f"[{self.role}] Tool '{name}' returned {type(result)}, expected Command.")
            return [ToolMessage(content=f"Tool '{name}' returned an invalid result.", tool_call_id=tool_call["id"])], {}

        update = result.update
        return update.get("messages", []), {k: v for k, v in update.items() if k != "messages"}

    def _maybe_compact(self, messages: list) -> list:
        """Summarize history into a compact message if approaching the context limit."""
        threshold = self.context_window * 0.80
        current_tokens = count_tokens(self.model_name, messages)
        if current_tokens < threshold:
            return messages

        logger.warning(
            f"[{self.role}] Compacting context: {current_tokens} tokens >= {threshold:.0f} "
            f"(80% of {self.context_window})"
        )

        # messages[0] = system prompt, messages[1] = initial user prompt
        to_summarize = messages[2:]
        history_text = "\n\n".join(
            f"[{getattr(m, 'type', 'message').upper()}]: {m.content}" for m in to_summarize
        )

        summary_prompt = [
            SystemMessage(content="You are summarizing a prior conversation for an SRE agent."),
            HumanMessage(
                content=(
                    "Summarize the following conversation history, preserving all key findings, "
                    "actions taken, commands run, outputs observed, hypotheses formed, and current "
                    "state. Be detailed enough for the agent to continue without losing context.\n\n"
                    f"<history>\n{history_text}\n</history>"
                )
            ),
        ]
        summary_ai_msg = self.llm.inference(messages=summary_prompt, tools=None)

        compacted = messages[0:2] + [
            HumanMessage(
                content="Here's a summary of the previous conversation:\n\n" + summary_ai_msg.content
            )
        ]

        after_tokens = count_tokens(self.model_name, compacted)
        logger.warning(f"[{self.role}] Context compacted: {current_tokens} → {after_tokens} tokens")
        return compacted

    async def arun(self, starting_prompts: list) -> dict:
        messages = list(starting_prompts)
        state = {"submitted": False, "verdict": None}
        steps = 0
        max_no_submit_reminders = 3
        no_submit_reminders = 0
        max_loop_repeats = 3
        max_loop_reminders = 3
        recent_fps: deque = deque(maxlen=max_loop_repeats)
        loop_reminders = 0

        while True:
            ai_msg = self.llm.inference(messages=messages, tools=self.tools)
            messages.append(ai_msg)
            steps += 1
            thinking = ai_msg.additional_kwargs.get("reasoning_content") or ai_msg.additional_kwargs.get("thinking")
            if not thinking and isinstance(ai_msg.content, list):
                for block in ai_msg.content:
                    if isinstance(block, dict) and block.get("type") == "thinking":
                        thinking = block.get("thinking", "")
                        break
            if thinking:
                logger.info(f"[{self.role}] Thinking: {thinking}")
            logger.info(f"[{self.role}] AI: {ai_msg.content}")
            for tc in ai_msg.tool_calls:
                logger.info(f"[{self.role}] Tool call: {tc['name']}({tc['args']})")

            if not ai_msg.tool_calls:
                if state.get("submitted") or no_submit_reminders >= max_no_submit_reminders:
                    break
                no_submit_reminders += 1
                reminder = HumanMessage(
                    content=(
                        f"You have not submitted your answer yet. "
                        f"Please call `{self.submit_tool.name}` with your final answer before finishing."
                    )
                )
                messages.append(reminder)
                logger.warning(
                    f"[{self.role}] Agent stopped without submitting (reminder {no_submit_reminders}/{max_no_submit_reminders})."
                )
                continue

            results = await asyncio.gather(*[self._invoke_tool(tc) for tc in ai_msg.tool_calls])
            for tool_msgs, state_updates in results:
                messages.extend(tool_msgs)
                state.update(state_updates)
                for msg in tool_msgs:
                    logger.info(f"[{self.role}] Tool result ({msg.name if hasattr(msg, 'name') else '?'}): {msg.content}")

            if state.get("submitted"):
                break

            # Detect repetitive tool-call loops
            current_fp = frozenset(
                (tc["name"], repr(sorted(tc["args"].items())))
                for tc in ai_msg.tool_calls
            )
            recent_fps.append(current_fp)
            if len(recent_fps) == max_loop_repeats and len(set(recent_fps)) == 1:
                if loop_reminders >= max_loop_reminders:
                    logger.warning(
                        f"[{self.role}] Loop detected after {loop_reminders} reminders — breaking."
                    )
                    break
                loop_reminders += 1
                reminder = HumanMessage(
                    content=(
                        "You have been calling the same tool(s) with the same arguments repeatedly without making progress. "
                        "Please try a different approach — run a different command, inspect the system from another angle, "
                        f"or call `{self.submit_tool.name}` if you have gathered enough information to submit your answer."
                    )
                )
                messages.append(reminder)
                logger.warning(
                    f"[{self.role}] Loop detected — injecting reminder ({loop_reminders}/{max_loop_reminders})."
                )

            messages = self._maybe_compact(messages)

        logger.info(
            f"[{self.role}] Finished. submitted={state.get('submitted')}, "
            f"verdict={state.get('verdict')!r}, steps={steps}"
        )
        return {"messages": messages, "steps": steps, **state}
