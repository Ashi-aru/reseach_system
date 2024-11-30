from datetime import date


TODAY = date.today().strftime("%Y-%m-%d")


# responseはChatCompletionオブジェクトで、これをリストに格納してしまうと、jsonとして保存ができないので、
# ChatCompletionオブジェクトを再帰的に計算し、ネストした辞書に変換する関数を定義
def to_dict_recursive(chat_completion_object):
    d = {
        "id": chat_completion_object.id,
        "object": chat_completion_object.object,
        "created": chat_completion_object.created,
        "model": chat_completion_object.model,
        "system_fingerprint": chat_completion_object.system_fingerprint,
        "choices": [
            {
                "index": chat_completion_object.choices[0].index,
                "message":{
                    "role": chat_completion_object.choices[0].message.role,
                    "content": chat_completion_object.choices[0].message.content,
                },
                "logprobs":chat_completion_object.choices[0].logprobs,
                "finish_reason":chat_completion_object.choices[0].finish_reason,
            }
        ],
        "usage": {
            "prompt_tokens": chat_completion_object.usage.prompt_tokens,
            "completion_tokens": chat_completion_object.usage.completion_tokens,
            "total_tokens": chat_completion_object.usage.total_tokens,
            "completion_tokens_details": {
                "reasoning_tokens": chat_completion_object.usage.completion_tokens_details['reasoning_tokens']
            }
        },
        "time": TODAY
    }
    return d


