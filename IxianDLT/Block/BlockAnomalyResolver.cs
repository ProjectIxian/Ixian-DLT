// Copyright (C) 2017-2025 Ixian
// This file is part of Ixian DLT - www.github.com/ProjectIxian/Ixian-DLT
//
// Ixian DLT is free software: you can redistribute it and/or modify
// it under the terms of the MIT License as published
// by the Open Source Initiative.
//
// Ixian DLT is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// MIT License for more details.

using System.Collections.Generic;

namespace DLT
{
    public static class BlockAnomalyResolver
    {
        private readonly static Dictionary<ulong, Dictionary<string, string>> anomalies = new() {
            { 4653587, new() { { "4653584-PSakv2VLGvZPggUVzrRvFpgRzADQYxCrT4UuZPrPetXZzNgkVuz7rJAwWVKT", "PoW transaction which has a solution for a forked block #4653584 and was not re-verified when including it in the block #4653587 after chain reorg due to an optimization bug. Forked block data: {\"hash\":\"c76d276fda251652f5ebd350746209855048f452a7362f8c99e795ad4c0f72a22ab12c9e18cae47d98fec625ea8f9ba31568bda951c7732bcf3be24fdb0c1aee\",\"wsChecksum\":\"1fc5d1e6c8ea373f299e476dc81fc0580c80b4e53859bd296b4c366ba5a8f9426e6b0bee4527d1c6c4d2c72b208241e4ebeb6d92ea70dddad9cead658f97a6c8\",\"sigFreezeChecksum\":\"50d67e0d54562afeb4cf46562f4768db9cbf30aeaa1cfe370cfadf58ad53fc47b1e02968bf6f94f6f18832117f4006d43472667eb2afe8da6246722a1fa34a0b\",\"difficulty\":\"18443817930783443811\",\"sigCount\":\"92\",\"txCount\":\"21\",\"txAmount\":\"38837.92740983\",\"timestamp\":\"1735164317\",\"version\":\"12\",\"hashrate\":\"6885775\",\"blocktime\":\"4185\",\"totalSignerDifficulty\":\"4992888014913.91328135\",\"sigRequired\":\"78\",\"requiredSignerDifficulty\":\"3222559629197.81400313\",\"sigChecksum\":\"a201ccae47ddd88b438fdaa749c41267ebf9aa9ca900e14ea81abde985ac8ea69f90e124647cecf69382c160a617a64e7fab5122da5d7f0185f5c3c1903b1512\"}" } } }
        };

        public static string? getTransactionAnomaly(ulong blockHeight, string txid)
        {
            if (anomalies.ContainsKey(blockHeight))
            {
                if (anomalies[blockHeight].ContainsKey(txid))
                {
                    return anomalies[blockHeight][txid];
                }
            }
            return null;
        }
    }
}
