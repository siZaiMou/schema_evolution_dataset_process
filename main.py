def main():
    import sys
    # 读取输入的数字
    input_number = int(sys.stdin.readline())
    number_str = str(input_number)
    str_length = len(number_str)

    # 生成指定范围内的质数列表（埃氏筛法）
    def generate_primes(max_value):
        is_prime_list = [True] * (max_value + 1)
        is_prime_list[0] = is_prime_list[1] = False
        for i in range(2, int(max_value ** 0.5) + 1):
            if is_prime_list[i]:
                for j in range(i * i, max_value + 1, i):
                    is_prime_list[j] = False
        return is_prime_list

    # 最大可能的数位和是9*19=171
    prime_flags = generate_primes(171)

    # 初始化记忆化数组
    # dp[位置][前一位+1][是否受限][是否开始][当前和] = -1(未知), 0(否), 1(是)
    memo = [[[[[-1 for _ in range(172)] for __ in range(2)] for ___ in range(2)] for ____ in range(11)] for _____ in
            range(str_length + 1)]

    # 深度优先搜索判断是否存在符合条件的数
    def dfs_search(position, last_digit, is_limited, has_started, current_sum):
        if current_sum > 171:
            return False
        # 到达数字末尾
        if position == str_length:
            # 必须已经开始(非零)且数位和为质数
            return has_started == 1 and prime_flags[current_sum]

        # 计算前一位的索引（将-1转换为0）
        last_idx = last_digit + 1
        # 检查记忆
        if memo[position][last_idx][is_limited][has_started][current_sum] != -1:
            return memo[position][last_idx][is_limited][has_started][current_sum] == 1

        # 确定当前位的最大可能数字
        max_digit = int(number_str[position]) if is_limited else 9

        # 从大到小尝试数字，以便后续构造最大数
        for digit in range(max_digit, -1, -1):
            # 如果已经开始且当前数字小于前一位，不符合非递减要求
            if has_started == 1 and digit < last_digit:
                continue

            # 计算新的受限状态
            new_limited = 1 if (is_limited == 1 and digit == max_digit) else 0
            # 计算新的开始状态
            new_started = 1 if (has_started == 1 or digit != 0) else 0
            # 计算新的数位和
            new_sum = current_sum + (digit if new_started == 1 else 0)

            # 递归检查下一位
            if dfs_search(position + 1, digit if new_started == 1 else last_digit,
                          new_limited, new_started, new_sum):
                memo[position][last_idx][is_limited][has_started][current_sum] = 1
                return True

        # 没有找到符合条件的数字
        memo[position][last_idx][is_limited][has_started][current_sum] = 0
        return False

    # 检查是否存在解
    if not dfs_search(0, -1, 1, 0, 0):
        print(-1)
        return

    # 回溯构造最大符合条件的数
    result_digits = []

    def build_result(position, last_digit, is_limited, has_started, current_sum):
        if position == str_length:
            return

        # 确定当前位的最大可能数字
        max_digit = int(number_str[position]) if is_limited else 9

        # 从大到小选择数字，确保结果最大
        for digit in range(max_digit, -1, -1):
            if has_started == 1 and digit < last_digit:
                continue

            new_limited = 1 if (is_limited == 1 and digit == max_digit) else 0
            new_started = 1 if (has_started == 1 or digit != 0) else 0
            new_sum = current_sum + (digit if new_started == 1 else 0)

            # 检查该数字是否能通向有效解
            if dfs_search(position + 1, digit if new_started == 1 else last_digit,
                          new_limited, new_started, new_sum):
                result_digits.append(str(digit))
                build_result(position + 1, digit if new_started == 1 else last_digit,
                             new_limited, new_started, new_sum)
                return

    # 构建结果
    build_result(0, -1, 1, 0, 0)

    # 处理前导零
    result_str = ''.join(result_digits)
    first_non_zero = 0
    while first_non_zero < len(result_str) and result_str[first_non_zero] == '0':
        first_non_zero += 1

    final_result = result_str[first_non_zero:] if first_non_zero < len(result_str) else ''

    # 输出结果
    if not final_result:
        print(-1)
    else:
        print(final_result)


if __name__ == "__main__":
    main()
